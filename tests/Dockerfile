FROM rust:1.75

WORKDIR /home/message-queue

# The minimum number of files required to allow `cargo fetch` to cache all dependencies.
# We do this separately from the `COPY ./ .` so most code changes don't invalidate the `cargo fetch` layer.
COPY Cargo.lock Cargo.toml ./
COPY src/lib.rs src/lib.rs
COPY src/main.rs src/main.rs
COPY rumqttd-protocol/ rumqttd-protocol/
COPY .cargo/config.toml .cargo/config.toml
COPY fuzz/Cargo.toml fuzz/Cargo.toml
COPY fuzz/fuzz_targets/ fuzz/fuzz_targets

RUN mkdir -p -m 0600 /root/.ssh
RUN ssh-keyscan github.com > /root/.ssh/known_hosts
COPY --chmod=0400 docker-util/ssh_config /etc/ssh/ssh_config.d/github.com.conf

ENV CARGO_UNSTABLE_SPARSE_REGISTRY=true

# We use the host's SSH agent here because the project has a dependency on TCE's
# private git repo. The host's agent is expected to be able to provide an
# appropriate key.
# See https://docs.docker.com/engine/reference/builder/#run---mounttypessh
# And https://www.ssh.com/academy/ssh/agent#ssh-agent-forwarding
#
# Test that SSH forwarding is working or explain why it isn't.
RUN --mount=type=ssh --mount=target=docker-util/,source=docker-util/ bash docker-util/test-ssh.sh

# Download all our dependencies in its own layer that's unlikely to be invalidated.
RUN --mount=type=ssh cargo fetch

# Build the biggest deps in a separately cached layer.
RUN cargo build --release -p tokio -p tracing -p tashi-consensus-engine

COPY ./ .

WORKDIR /home/message-queue

RUN cargo build --release
