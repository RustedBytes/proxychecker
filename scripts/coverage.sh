#!/usr/bin/env bash
# Measure all production Rust modules using both unit and Python API tests.
set -euo pipefail
cd "$(dirname "$0")/.."

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target/coverage}"
# Rust unit tests embed Python; some distributions need its shared-library path.
python_libdir="$(python -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')"
export LD_LIBRARY_PATH="$python_libdir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
source <(cargo llvm-cov show-env --sh)
cargo llvm-cov clean --workspace
cargo test --locked
maturin build --locked --out "$CARGO_TARGET_DIR/wheels"
python -m pip install --force-reinstall "$CARGO_TARGET_DIR"/wheels/*.whl
python -m unittest discover -s tests -v

mkdir -p "$CARGO_TARGET_DIR/report"
cargo llvm-cov report --json --output-path "$CARGO_TARGET_DIR/report/coverage.json"
cargo llvm-cov report --html --output-dir "$CARGO_TARGET_DIR/report/html"
python scripts/coverage_summary.py "$CARGO_TARGET_DIR/report/coverage.json"
cargo llvm-cov report --fail-under-lines 90
