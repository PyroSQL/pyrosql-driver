# Stage 1: Build the Rust FFI library
FROM rust:1.85-slim-bookworm AS ffi-builder
RUN apt-get update && apt-get install -y pkg-config libssl-dev git && rm -rf /var/lib/apt/lists/*
WORKDIR /build
RUN git clone --depth 1 https://github.com/pyrosql/pyrosql-driver.git .
RUN cargo build --release --manifest-path ffi-pwire/Cargo.toml && \
    find . -path "*/release/*.so" -exec strip {} \; && \
    find . -path "*/release/*.so" -exec cp {} /build/libpyrosql_ffi_pwire.so \;

# Stage 2: Build the unified PHP extension
FROM php:8.4-cli AS ext-builder
RUN apt-get update && apt-get install -y autoconf gcc make pkg-config && rm -rf /var/lib/apt/lists/*
COPY --from=ffi-builder /build/libpyrosql_ffi_pwire.so /usr/lib/
RUN ldconfig
WORKDIR /usr/src/pyrosql
COPY config.m4 php_pyrosql.h pyrosql_ext.c pyrosql_stmt.c pyrosql_native.c ./
RUN phpize && ./configure --enable-pyrosql && make -j$(nproc)

# Stage 3: Artifacts
FROM scratch AS artifacts
COPY --from=ext-builder /usr/src/pyrosql/modules/pyrosql.so /pyrosql.so
COPY --from=ffi-builder /build/libpyrosql_ffi_pwire.so /libpyrosql_ffi_pwire.so
