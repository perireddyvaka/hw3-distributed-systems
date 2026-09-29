#!/usr/bin/env bash
# =============================================================================
#  setup_cluster.sh — RCE Cluster Environment Setup for HW3 gRPC Assignment
# =============================================================================
#  Prepares the RCE cluster environment for execution of:
#    1. bash run_submission.sh
#    2. Multi-node distributed execution across Slurm nodes
#
#  Usage:
#    bash setup_cluster.sh            # Standard setup (creates/uses .venv)
#    bash setup_cluster.sh --no-venv  # Use system/user python environment
#    bash setup_cluster.sh --check    # Check environment without installing
# =============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_ROOT"

# ── ANSI Colors ───────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}[setup_cluster]${RESET} $*"; }
ok()      { echo -e "${GREEN}[  OK  ]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[ WARN ]${RESET} $*"; }
fail()    { echo -e "${RED}[ FAIL ]${RESET} $*"; }
section() { echo -e "\n${BOLD}${YELLOW}══ $* ══${RESET}"; }

# ── Argument Handling ────────────────────────────────────────────────────────
USE_VENV=1
FORCE_VENV=0
CHECK_ONLY=0

for arg in "$@"; do
    case "$arg" in
        --no-venv)     USE_VENV=0 ;;
        --force-venv)  FORCE_VENV=1; USE_VENV=1 ;;
        --check)       CHECK_ONLY=1 ;;
        --help|-h)
            echo "Usage: bash setup_cluster.sh [--no-venv] [--force-venv] [--check]"
            exit 0
            ;;
        *)
            warn "Unknown argument: $arg (ignoring)"
            ;;
    esac
done

echo -e "${BOLD}${CYAN}============================================================${RESET}"
echo -e "${BOLD}${CYAN}   RCE Cluster Setup — HW3 Real-Time Weather Analytics     ${RESET}"
echo -e "${BOLD}${CYAN}============================================================${RESET}"
info "Repository root: $REPO_ROOT"
info "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"

# =============================================================================
#  PHASE 1: Toolchain & HPC Modules
# =============================================================================
section "Phase 1: Environment & Toolchain Detection"

# Check for environment modules (common on RCE cluster)
if command -v module >/dev/null 2>&1; then
    info "Environment Modules detected (HPC cluster mode)."
    # Attempt to load standard modules if gcc or python are missing
    if ! command -v g++ >/dev/null 2>&1; then
        info "Loading gcc module..."
        module load gcc 2>/dev/null || module load gcc/9.3.0 2>/dev/null || true
    fi
    if ! command -v python3 >/dev/null 2>&1; then
        info "Loading python3 module..."
        module load python 2>/dev/null || module load python/3.10 2>/dev/null || true
    fi
fi

# 1. Check Python 3
if ! command -v python3 >/dev/null 2>&1; then
    fail "python3 not found in PATH. Please load python3 module or install Python >= 3.8."
    exit 1
fi

PY_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PY_MAJOR=$(echo "$PY_VER" | cut -d. -f1)
PY_MINOR=$(echo "$PY_VER" | cut -d. -f2)

if [ "$PY_MAJOR" -lt 3 ] || ([ "$PY_MAJOR" -eq 3 ] && [ "$PY_MINOR" -lt 8 ]); then
    fail "Python version >= 3.8 is required (found Python $PY_VER)."
    exit 1
fi
ok "Python $PY_VER detected at $(which python3)"

# 2. Check C++ Compiler (g++)
if ! command -v g++ >/dev/null 2>&1; then
    fail "g++ compiler not found. Required for HW2 sequential reference oracle."
    exit 1
fi
ok "C++ compiler detected: $(g++ --version | head -1)"

# =============================================================================
#  PHASE 2: Python Virtual Environment & Packages
# =============================================================================
section "Phase 2: Python Environment & Dependencies"

VENV_DIR="$REPO_ROOT/.venv"
PY_EXEC="$(which python3)"
PIP_EXEC="$(which pip || which pip3 || echo true)"

# First check: Are required packages already installed in system/user environment?
ALREADY_INSTALLED=0
if "$PY_EXEC" -c "import grpc, google.protobuf, pytest, matplotlib, pandas, numpy, psutil" 2>/dev/null; then
    ALREADY_INSTALLED=1
fi

if [ "$ALREADY_INSTALLED" -eq 1 ] && [ "$FORCE_VENV" -eq 0 ]; then
    ok "All required Python packages are already installed in current environment."
    info "Skipping redundant package download."
else
    if [ "$USE_VENV" -eq 1 ]; then
        if [ ! -d "$VENV_DIR" ]; then
            info "Creating virtual environment at $VENV_DIR ..."
            python3 -m venv "$VENV_DIR"
        fi
        # Activate virtual environment
        # shellcheck disable=SC1091
        source "$VENV_DIR/bin/activate"
        ok "Virtual environment active: $VENV_DIR"
        PY_EXEC="$VENV_DIR/bin/python3"
        PIP_EXEC="$VENV_DIR/bin/pip"
    fi

    if [ "$CHECK_ONLY" -eq 0 ]; then
        info "Installing Python dependencies from requirements.txt..."
        if "$PIP_EXEC" install -r "$REPO_ROOT/requirements.txt"; then
            ok "All Python requirements installed successfully."
        else
            warn "Direct install hit permission issues, attempting with --user flag..."
            "$PIP_EXEC" install --user -r "$REPO_ROOT/requirements.txt"
            ok "Python requirements installed via --user."
        fi
    fi
fi

# Verify required packages are importable
info "Verifying core Python packages..."
"$PY_EXEC" -c "
import grpc
import google.protobuf
import pytest
import matplotlib
import pandas
import numpy
import psutil
print('All core packages importable.')
"
ok "gRPC, Protobuf, Pytest, Matplotlib, Pandas, NumPy, Psutil verified."

# =============================================================================
#  PHASE 3: Build Reference HW2 C++ Oracle
# =============================================================================
section "Phase 3: Building Reference HW2 Sequential Oracle"

ORACLE_SRC="$REPO_ROOT/hw2_mpi/src/q8_seq.cpp"
ORACLE_BIN="$REPO_ROOT/hw2_oracle/sequential_oracle"

mkdir -p "$REPO_ROOT/hw2_oracle"

if [ ! -f "$ORACLE_SRC" ]; then
    fail "HW2 sequential source not found at: $ORACLE_SRC"
    exit 1
fi

info "Compiling $ORACLE_SRC with g++ -O3 -std=c++17 ..."
g++ -O3 -std=c++17 -Wall -Wextra -o "$ORACLE_BIN" "$ORACLE_SRC"
chmod +x "$ORACLE_BIN"
ok "Compiled HW2 oracle binary: $ORACLE_BIN"

# =============================================================================
#  PHASE 4: Generate Protocol Buffer Stubs
# =============================================================================
section "Phase 4: Compiling gRPC Protocol Buffers"

info "Generating Python gRPC stubs from hw3_grpc/proto/weather.proto..."
export PYTHON="$PY_EXEC"
bash "$REPO_ROOT/hw3_grpc/scripts/generate_proto.sh"
ok "Protobuf stubs generated and import-patched."

# Also ensure stubs exist in submission/proto/
mkdir -p "$REPO_ROOT/submission/proto"
cp -f "$REPO_ROOT/hw3_grpc/proto/weather.proto" "$REPO_ROOT/submission/proto/"

# =============================================================================
#  PHASE 5: Slurm Allocation & Node Discovery
# =============================================================================
section "Phase 5: Slurm / RCE Cluster Node Discovery"

SLURM_ACTIVE=0
SLURM_NODES=()

if [ -n "${SLURM_JOB_NODELIST:-}" ] || [ -n "${SLURM_NODELIST:-}" ]; then
    SLURM_ACTIVE=1
    NODE_VAR="${SLURM_JOB_NODELIST:-$SLURM_NODELIST}"
    if command -v scontrol >/dev/null 2>&1; then
        mapfile -t SLURM_NODES < <(scontrol show hostnames "$NODE_VAR")
    else
        # Fallback if scontrol not directly available
        IFS=',' read -ra SLURM_NODES <<< "$NODE_VAR"
    fi
fi

if [ "$SLURM_ACTIVE" -eq 1 ] && [ "${#SLURM_NODES[@]}" -gt 0 ]; then
    ok "Slurm active! Job ID: ${SLURM_JOB_ID:-unknown}, Nodes: ${#SLURM_NODES[@]}"
    info "Allocated compute nodes:"
    for idx in "${!SLURM_NODES[@]}"; do
        echo "    [$idx] ${SLURM_NODES[$idx]}"
    done
    
    COORD_NODE="${SLURM_NODES[0]}"
    info "Suggested Multi-Node Topology:"
    echo "    • Coordinator node: $COORD_NODE"
    if [ "${#SLURM_NODES[@]}" -gt 1 ]; then
        echo "    • Worker / Client nodes: ${SLURM_NODES[*]:1}"
    else
        echo "    • Single compute node allocated (local multi-worker mode)"
    fi
else
    info "Not running inside a Slurm allocation (or on single host / login node)."
    info "To request compute nodes on RCE, run:"
    echo "    salloc --nodes=3 --ntasks-per-node=1"
    echo "    scontrol show hostnames \$SLURM_JOB_NODELIST"
fi

# =============================================================================
#  PHASE 6: Generate Reusable Cluster Environment Helper
# =============================================================================
section "Phase 6: Generating cluster_env.sh"

ENV_FILE="$REPO_ROOT/cluster_env.sh"

cat <<EOF > "$ENV_FILE"
#!/usr/bin/env bash
# Auto-generated by setup_cluster.sh on $(date)
# Source this file to configure environment: source cluster_env.sh

export REPO_ROOT="$REPO_ROOT"
export PROJECT_ROOT="$REPO_ROOT"
export PYTHONPATH="\$REPO_ROOT:\${PYTHONPATH:-}"

# Virtual environment activation
if [ -f "$VENV_DIR/bin/activate" ]; then
    source "$VENV_DIR/bin/activate"
fi

# Export Slurm node variables if present
if [ -n "\${SLURM_JOB_NODELIST:-}" ] && command -v scontrol >/dev/null 2>&1; then
    export RCE_NODES=\$(scontrol show hostnames "\$SLURM_JOB_NODELIST" | tr '\\n' ' ')
    export RCE_COORD_NODE=\$(echo "\$RCE_NODES" | awk '{print \$1}')
fi

export HW3_COORDINATOR_PORT=\${HW3_COORDINATOR_PORT:-50050}
export HW3_WORKER_BASE_PORT=\${HW3_WORKER_BASE_PORT:-50060}
EOF

chmod +x "$ENV_FILE"
ok "Created environment helper: $ENV_FILE"

# =============================================================================
#  PHASE 7: Smoke Test Verification
# =============================================================================
section "Phase 7: System Smoke Test"

info "Verifying gRPC server imports and basic components..."
export PYTHONPATH="$REPO_ROOT:${PYTHONPATH:-}"
"$PY_EXEC" -c "
import sys
from hw3_grpc.generated import weather_pb2, weather_pb2_grpc
from hw3_grpc.coordinator.server import CoordinatorServicer
from hw3_grpc.coordinator.dispatcher import Dispatcher
from hw3_grpc.common.analytics import AnalyticsAccumulator
from hw3_grpc.common.models import WeatherRecord
from hw3_grpc.common.aggregation import merge

acc = AnalyticsAccumulator(k=10)
rec = WeatherRecord(
    timestamp=1600000000, station_id=1, temperature=25.0,
    humidity=50.0, pressure=1013.25, rainfall=5.0, wind_speed=15.0
)
acc.add(rec)
snap = acc.snapshot()
assert snap.total_measurements == 1, 'Accumulator measurement count mismatch'
print('  Smoke test PASSED: accumulator, protobuf, and gRPC modules operational.')
"
ok "End-to-end smoke test passed successfully."

# =============================================================================
#  PHASE 8: Complete Summary
# =============================================================================
echo ""
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${GREEN}  RCE CLUSTER SETUP COMPLETE — READY TO RUN${RESET}"
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════════════${RESET}"
echo -e "  ${BOLD}Virtualenv:${RESET}      $VENV_DIR"
echo -e "  ${BOLD}HW2 Oracle:${RESET}      $ORACLE_BIN"
echo -e "  ${BOLD}Environment:${RESET}     source cluster_env.sh"
echo ""
echo -e "  ${BOLD}Next Steps:${RESET}"
echo -e "  1. To run all tests, benchmarks & generate submission:"
echo -e "     ${CYAN}bash run_submission.sh${RESET}"
echo ""
echo -e "  2. For fast verification (~3 min):"
echo -e "     ${CYAN}bash run_submission.sh --skip-benchmarks${RESET}"
echo ""
echo -e "  3. For multi-node RCE cluster execution:"
echo -e "     ${CYAN}bash hw3_grpc/scripts/run_rce_cluster.sh${RESET}"
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════════════${RESET}"
