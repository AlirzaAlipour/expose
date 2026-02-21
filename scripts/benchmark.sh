#!/bin/bash

set -euo pipefail

echo "Running zero-copy benchmarks..."

echo "=== Standard path ==="
cargo bench --no-default-features

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  echo "=== io_uring path ==="
  cargo bench --features io_uring
fi

echo "Benchmark runs complete."
