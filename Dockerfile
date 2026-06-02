FROM rust:1.89-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY crates ./crates
COPY tools ./tools
COPY vendor ./vendor

RUN cargo build --release --bin rstock

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/rstock /usr/local/bin/rstock
COPY config.example.toml /app/config.example.toml

EXPOSE 8080

CMD ["rstock"]
