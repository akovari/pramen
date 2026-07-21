# Pramen release binary in a minimal runtime image.
#
# Build from the repo root (worktree or clone):
#   docker build -f deploy/container/Dockerfile -t pramen:local .
#
# Base tags are pinned exactly (CKV_DOCKER_7 / supply-chain).

FROM rust:1.97.0-bookworm AS build
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends proto-compiler pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo ./.cargo
COPY crates ./crates
RUN cargo build --release -p pramen \
    && strip target/release/pramen

FROM debian:bookworm-20260112-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /var/lib/pramen --create-home --shell /usr/sbin/nologin pramen \
    && mkdir -p /etc/pramen /var/lib/pramen/checkpoints /var/lib/pramen/ledger \
    && chown -R pramen:pramen /var/lib/pramen

COPY --from=build /src/target/release/pramen /usr/local/bin/pramen

USER pramen
WORKDIR /var/lib/pramen
ENTRYPOINT ["/usr/local/bin/pramen"]
CMD ["--help"]
