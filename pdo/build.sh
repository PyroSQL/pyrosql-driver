#!/bin/bash
set -e

echo "=== Building libpyrosql_ffi_pwire.so ==="
cd /var/pyrosql-driver
cargo build --release --manifest-path ffi-pwire/Cargo.toml
strip target/release/libpyrosql_ffi_pwire.so
cp target/release/libpyrosql_ffi_pwire.so /usr/lib/
ldconfig

echo "=== Building pdo_pyrosql extension ==="
cd /var/pdo_pyrosql
phpize
./configure --enable-pdo-pyrosql
make clean 2>/dev/null || true
make -j$(nproc)
make install
echo "extension=pdo_pyrosql.so" > /usr/local/etc/php/conf.d/pdo_pyrosql.ini

echo "=== Verifying ==="
php -m | grep pdo_pyrosql && echo "SUCCESS: pdo_pyrosql loaded" || echo "FAIL: pdo_pyrosql not loaded"
