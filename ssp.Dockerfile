FROM rust:1.92-slim-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
      libprotobuf-dev protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock* ./
COPY vendor/breez-sdk ./vendor/breez-sdk
COPY vendor/spark/signer ./vendor/spark/signer
COPY vendor/spark/protos ./vendor/spark/protos
RUN mkdir src \
 && echo 'fn main() {}' > src/main.rs \
 && cargo build --release \
 && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 ssp \
 && useradd --system --uid 10001 --gid ssp --home-dir /data ssp \
 && install -d -o ssp -g ssp /data
COPY --from=builder /app/target/release/open-ssp /usr/local/bin/open-ssp
ENV SSP_DATA_DIR=/data
USER ssp
EXPOSE 5000
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD ["open-ssp", "healthcheck"]
ENTRYPOINT ["open-ssp"]
