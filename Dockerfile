# Pillar images: one build, two targets.
#
#   docker build --target namenode -t pillar/namenode:dev .
#   docker build --target datanode -t pillar/datanode:dev .
#
# The Helm chart and the system tests start the binaries through these
# entrypoints and pass everything else as arguments.

FROM rust:1.96-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p pillar-namenode -p pillar-datanode --bins \
    && mkdir /out && cp target/release/pillar-namenode target/release/pillar-datanode /out/

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

FROM runtime AS namenode
COPY --from=build /out/pillar-namenode /usr/local/bin/
EXPOSE 9000 9870
ENTRYPOINT ["/usr/local/bin/pillar-namenode"]

FROM runtime AS datanode
COPY --from=build /out/pillar-datanode /usr/local/bin/
EXPOSE 9867 50010 50075
ENTRYPOINT ["/usr/local/bin/pillar-datanode"]
