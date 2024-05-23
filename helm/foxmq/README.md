Helm Chart: `foxmq-metrics`
=========================

This chart is for easily spinning up a stress test benchmark run in Kubernetes.

When installed, it will spin up InfluxDB and Redis pods, then once those are ready,
it will automatically start the foxmq-metrics.

The configuration options for this chart and their defaults are documented
in `values.yaml` in this directory.

It is by default configured for running against the cluster I (@abonander) set up
in Azure, `cluster-tashi-perf`.
