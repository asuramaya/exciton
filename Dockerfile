FROM rust:1.88-bookworm AS chef
RUN cargo install cargo-chef --version 0.1.77 --locked
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples
COPY deploy ./deploy
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples
COPY deploy ./deploy
RUN cargo build --release --bin exciton

FROM debian:bookworm-slim

RUN apt-get update     && apt-get install -y --no-install-recommends         ca-certificates         git         openssh-client         fonts-dejavu-core         fontconfig         chromium         libnss3         libgbm1         libasound2         libatk1.0-0         libatk-bridge2.0-0         libcups2         libxkbcommon0         libxcomposite1         libxdamage1         libxfixes3         libxrandr2         libpango-1.0-0         libcairo2     && git config --global --add safe.directory /srv/publisher-target     && rm -rf /var/lib/apt/lists/*

WORKDIR /data

COPY --from=builder /app/target/release/exciton /usr/local/bin/exciton
COPY deploy/config.container.toml.example /etc/exciton/config.toml

ENV EXCITON_DB_PATH=/data/exciton.db

VOLUME ["/data", "/srv/publisher-target", "/root/.ssh"]

ENTRYPOINT ["exciton"]
CMD ["/etc/exciton/config.toml"]
