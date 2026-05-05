FROM rust:1.95-bookworm AS chef
RUN cargo install cargo-chef --version 0.1.77 --locked
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples
COPY deploy ./deploy
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples
COPY deploy ./deploy
COPY crates ./crates
# Build both binaries: exciton (the engine) + claw (the agent).
RUN cargo build --release --bin exciton && \
    cargo build --release -p exciton-claw --bin claw

FROM debian:bookworm-slim

RUN apt-get update     && apt-get install -y --no-install-recommends         ca-certificates         git         openssh-client         fonts-dejavu-core         fontconfig         chromium         libnss3         libgbm1         libasound2         libatk1.0-0         libatk-bridge2.0-0         libcups2         libxkbcommon0         libxcomposite1         libxdamage1         libxfixes3         libxrandr2         libpango-1.0-0         libcairo2     && rm -rf /var/lib/apt/lists/*

# Non-root runtime user. uid 10001 stays out of the host's normal user
# range so a typical bind-mount won't collide. Operators who need
# specific ownership on bind-mounted volumes can set --user on
# `docker run` to override.
RUN groupadd -r --gid 10001 exciton  && useradd -r --uid 10001 --gid exciton --create-home --home-dir /home/exciton --shell /usr/sbin/nologin exciton  && mkdir -p /data /srv/publisher-target /home/exciton/.ssh /home/exciton/.exciton  && chown -R exciton:exciton /data /srv/publisher-target /home/exciton

# git's safe.directory has to be set per-user; the publisher pushes from
# the runtime container so the exciton user owns the directive.
RUN su -s /bin/sh exciton -c "git config --global --add safe.directory /srv/publisher-target"

WORKDIR /data

COPY --from=builder /app/target/release/exciton /usr/local/bin/exciton
COPY --from=builder /app/target/release/claw /usr/local/bin/claw
COPY deploy/config.container.toml.example /etc/exciton/config.toml

ENV EXCITON_DB_PATH=/data/exciton.db

USER exciton

VOLUME ["/data", "/srv/publisher-target", "/home/exciton/.ssh", "/home/exciton/.exciton"]

ENTRYPOINT ["exciton"]
CMD ["/etc/exciton/config.toml"]
