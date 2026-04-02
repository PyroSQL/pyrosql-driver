FROM rust:1.85-slim-bookworm

WORKDIR /app
COPY . .

RUN cargo build --release 2>&1
RUN cargo test 2>&1

CMD ["./target/release/pyrosql-pooler", "--help"]
