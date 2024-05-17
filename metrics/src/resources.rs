use color_eyre::eyre;
use influxdb2::models::data_point::DataPointBuilder;
use influxdb2::models::DataPoint;
use sysinfo::{MemoryRefreshKind, Networks, Pid, ProcessRefreshKind, System};

#[derive(Debug)]
pub struct Resources {
    pub total_cpu_usage: f32,
    pub process_cpu_usage: f32,
    pub per_core_cpu_usage: Vec<(u32, f32)>,

    pub process_memory: u64,
    pub total_memory: u64,

    pub network_rx: u64,
    pub network_tx: u64,
}

pub struct ResourcesContext {
    pid: Pid,
    system: System,
    networks: Networks,
}

impl ResourcesContext {
    pub fn new() -> eyre::Result<Self> {
        let pid = match sysinfo::get_current_pid() {
            Ok(pid) => pid,
            Err(e) => eyre::bail!("failed to get process pid: {e:?}"),
        };

        let mut system = System::new();
        system.refresh_cpu_usage();
        system.refresh_memory_specifics(MemoryRefreshKind::new().with_ram());

        // Ensure we have current process info
        system.refresh_process_specifics(pid, ProcessRefreshKind::new().with_cpu().with_memory());
        let Some(_) = system.process(pid) else {
            eyre::bail!("failed to get process info");
        };

        let mut networks = Networks::new();
        // Required to load networks
        networks.refresh_list();
        networks.refresh();

        Ok(ResourcesContext {
            pid,
            system,
            networks,
        })
    }

    pub fn sample(&mut self) -> eyre::Result<Resources> {
        self.system.refresh_cpu_usage();
        self.system
            .refresh_memory_specifics(MemoryRefreshKind::new().with_ram());

        self.networks.refresh();

        self.system.refresh_process_specifics(
            self.pid,
            ProcessRefreshKind::new().with_cpu().with_memory(),
        );

        let Some(proc_info) = self.system.process(self.pid) else {
            eyre::bail!("failed to get process info");
        };

        let per_core_cpu_usage: Vec<(u32, f32)> = self
            .system
            .cpus()
            .iter()
            .enumerate()
            .map(|(i, cpu)| (i as u32, cpu.cpu_usage()))
            .collect();

        let mut network_rx = 0u64;
        let mut network_tx = 0u64;

        for (_, network) in self.networks.list() {
            network_rx = network_rx.saturating_add(network.received());
            network_tx = network_tx.saturating_add(network.transmitted());
        }

        let resources = Resources {
            total_cpu_usage: self.system.global_cpu_info().cpu_usage(),
            process_cpu_usage: proc_info.cpu_usage(),
            per_core_cpu_usage,
            process_memory: proc_info.memory(),
            total_memory: self.system.used_memory(),
            network_rx,
            network_tx,
        };

        resources.debug_log();

        Ok(resources)
    }
}

impl Resources {
    fn debug_log(&self) {
        tracing::debug!(
            total_cpu_usage = self.total_cpu_usage,
            process_cpu_usage = self.process_cpu_usage,
            per_core_cpu_usage = ?self.per_core_cpu_usage,
            process_memory = self.process_memory,
            total_memory = self.total_memory,
            network_rx = self.network_rx,
            network_tx = self.network_tx,
            "resources"
        );
    }

    pub fn to_datapoint(&self) -> DataPointBuilder {
        DataPoint::builder("resources").field("total_cpu_usage", self.total_cpu_usage.to_string())
    }
}
