FROM rust:1.82 AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/photon /usr/local/bin/photon
COPY config.example.toml /etc/photon/config.toml
VOLUME ["/data"]
ENTRYPOINT ["photon", "/etc/photon/config.toml"]
