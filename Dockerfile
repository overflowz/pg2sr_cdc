FROM rust:1.88-bookworm AS planner
WORKDIR /app
RUN cargo install cargo-chef --version 0.1.68 --locked
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM rust:1.88-bookworm AS builder
WORKDIR /app
RUN cargo install cargo-chef --version 0.1.68 --locked
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin pg2sr_cdc

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/pg2sr_cdc /usr/local/bin/pg2sr_cdc
ENTRYPOINT ["/usr/local/bin/pg2sr_cdc"]
