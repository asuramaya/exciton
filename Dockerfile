FROM rust:1.88-bookworm AS chef
RUN cargo install cargo-chef --version 0.1.77 --locked
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples
COPY deploy ./deploy
COPY assets ./assets
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples
COPY deploy ./deploy
COPY assets ./assets
RUN cargo build --release --bin photon

FROM debian:bookworm-slim

RUN apt-get update     && apt-get install -y --no-install-recommends         ca-certificates         git         openssh-client         fonts-dejavu-core         fontconfig     && git config --global --add safe.directory /srv/MadApes.ai     && rm -rf /var/lib/apt/lists/*

WORKDIR /data

COPY --from=builder /app/target/release/photon /usr/local/bin/photon
COPY deploy/config.container.toml.example /etc/photon/config.toml

ENV PHOTON_DB_PATH=/data/photon.db

VOLUME ["/data", "/srv/MadApes.ai", "/root/.ssh"]

ENTRYPOINT ["photon"]
CMD ["/etc/photon/config.toml"]
