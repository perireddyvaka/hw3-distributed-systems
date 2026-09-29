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

# ── Helper functions for toolchain version verification ──────────────────────
check_python_version() {
    local py_cand="$1"
    if [ ! -x "$py_cand" ] && ! command -v "$py_cand" >/dev/null 2>&1; then
        return 1
    fi
    local ver
    ver=$("$py_cand" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null) || return 1
    local major
    major=$(echo "$ver" | cut -d. -f1)
    local minor
    minor=$(echo "$ver" | cut -d. -f2)
    if [ "$major" -gt 3 ] || ([ "$major" -eq 3 ] && [ "$minor" -ge 8 ]); then
        echo "$ver"
        return 0
    fi
    return 1
}

check_gxx_cxx17() {
    local gxx_cand="$1"
    if ! command -v "$gxx_cand" >/dev/null 2>&1; then
        return 1
    fi
    echo "int main(){return 0;}" | "$gxx_cand" -x c++ -std=c++17 - -o /dev/null 2>/dev/null
}

LOADED_PY_MODULE=""
LOADED_GCC_MODULE=""
FOUND_PY=""
PY_VER=""

# =============================================================================
#  PHASE 1: Toolchain & HPC Modules (Dynamic Discovery)
# =============================================================================
section "Phase 1: Environment & Toolchain Detection"

# 1. First, test if default python3 satisfies >= 3.8
if command -v python3 >/dev/null 2>&1; then
    if PY_VER=$(check_python_version python3); then
        FOUND_PY="$(which python3)"
        ok "Default python3 ($PY_VER) satisfies requirement >= 3.8: $FOUND_PY"
    fi
fi

# 2. If default python3 is older (e.g. Python 3.6 on older RCE nodes), search dynamically:
if [ -z "$FOUND_PY" ]; then
    DEF_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo "not found")
    warn "Default python3 ($DEF_VER) is older than 3.8. Scanning for compatible Python on cluster..."

    # 2a. Check Environment Modules (Lmod / Environment Modules on HPC)
    if command -v module >/dev/null 2>&1; then
        info "Searching Environment Modules for Python >= 3.8..."
        # Query module avail for candidate modules
        AVAIL_MODS=$(module -t avail 2>&1 | grep -iE '^(python|miniconda|anaconda)' | tr '\n' ' ' || true)
        CANDIDATE_MODULES=(
            "python/3.11" "python/3.10" "python/3.9" "python/3.8" "python/3.12"
            "python3/3.10" "python3/3.9" "python3/3.8" "python3" "python"
            "miniconda3" "anaconda3" "miniconda" "anaconda"
        )
        for mod in "${CANDIDATE_MODULES[@]}" $AVAIL_MODS; do
            if [ -z "$mod" ]; then continue; fi
            if module load "$mod" 2>/dev/null; then
                if PY_VER=$(check_python_version python3); then
                    FOUND_PY="$(which python3)"
                    LOADED_PY_MODULE="$mod"
                    ok "Loaded cluster module '$mod' → Python $PY_VER active ($FOUND_PY)"
                    break
                fi
            fi
        done
    fi

    # 2b. Check explicit binary names in PATH
    if [ -z "$FOUND_PY" ]; then
        for bin in python3.12 python3.11 python3.10 python3.9 python3.8; do
            if command -v "$bin" >/dev/null 2>&1; then
                if PY_VER=$(check_python_version "$bin"); then
                    FOUND_PY="$(which "$bin")"
                    ok "Found compatible Python binary in PATH: $FOUND_PY ($PY_VER)"
                    break
                fi
            fi
        done
    fi

    # 2c. Check standard filesystem paths (Conda, Software Collections, /usr/local, /opt)
    if [ -z "$FOUND_PY" ]; then
        SEARCH_PATHS=(
            "$HOME/miniconda3/bin/python3"
            "$HOME/anaconda3/bin/python3"
            "$HOME/.conda/envs/*/bin/python3"
            "/opt/rh/rh-python38/root/usr/bin/python3"
            "/opt/rh/rh-python39/root/usr/bin/python3"
            "/opt/rh/rh-python310/root/usr/bin/python3"
            "/usr/local/bin/python3.10"
            "/usr/local/bin/python3.9"
            "/usr/local/bin/python3.8"
            "/usr/bin/python3.10"
            "/usr/bin/python3.9"
            "/usr/bin/python3.8"
            "/opt/python/*/bin/python3"
        )
        for sp in "${SEARCH_PATHS[@]}"; do
            for p in $sp; do
                if [ -x "$p" ]; then
                    if PY_VER=$(check_python_version "$p"); then
                        FOUND_PY="$p"
                        ok "Found compatible Python binary at: $FOUND_PY ($PY_VER)"
                        break 2
                    fi
                fi
            done
        done
    fi
fi

# 3. Final verification for Python
if [ -z "$FOUND_PY" ]; then
    fail "No Python >= 3.8 found on this cluster node."
    echo "    On the RCE cluster, please run one of:"
    echo "        module avail python"
    echo "        module load python/3.10   (or python/3.8, anaconda3, etc.)"
    exit 1
fi

ok "Selected Python for cluster execution: $FOUND_PY (version $PY_VER)"

# 4. Check C++ Compiler with C++17 support
if ! check_gxx_cxx17 g++; then
    warn "Default g++ does not support -std=c++17. Searching for modern GCC module..."
    if command -v module >/dev/null 2>&1; then
        GCC_MODULES=("gcc/9.3.0" "gcc/10.2.0" "gcc/11.2.0" "gcc/8.3.0" "gcc" "devtoolset-9" "devtoolset-8" "devtoolset-10")
        AVAIL_GCC=$(module -t avail 2>&1 | grep -iE '^(gcc|devtoolset)' | tr '\n' ' ' || true)
        for m in "${GCC_MODULES[@]}" $AVAIL_GCC; do
            if [ -z "$m" ]; then continue; fi
            if module load "$m" 2>/dev/null; then
                if check_gxx_cxx17 g++; then
                    LOADED_GCC_MODULE="$m"
                    ok "Loaded GCC module '$m' → $(g++ --version | head -1)"
                    break
                fi
            fi
        done
    fi
fi

if ! check_gxx_cxx17 g++; then
    fail "g++ with C++17 support not found. Please run 'module load gcc' or load a devtoolset."
    exit 1
fi
ok "C++ compiler verified: $(g++ --version | head -1)"

# =============================================================================
#  PHASE 2: Python Virtual Environment & Packages
# =============================================================================
section "Phase 2: Python Environment & Dependencies"

VENV_DIR="$REPO_ROOT/.venv"
PY_EXEC="$FOUND_PY"
PIP_EXEC="$(dirname "$PY_EXEC")/pip"
if [ ! -x "$PIP_EXEC" ]; then
    PIP_EXEC="$(which pip3 || which pip || echo true)"
fi

# First check: Are required packages already installed in this Python?
ALREADY_INSTALLED=0
if "$PY_EXEC" -c "import grpc, google.protobuf, pytest, matplotlib, pandas, numpy, psutil" 2>/dev/null; then
    ALREADY_INSTALLED=1
fi

if [ "$ALREADY_INSTALLED" -eq 1 ] && [ "$FORCE_VENV" -eq 0 ]; then
    ok "All required Python packages are already present in $PY_EXEC."
    info "Skipping redundant package download."
else
    if [ "$USE_VENV" -eq 1 ]; then
        if [ ! -d "$VENV_DIR" ]; then
            info "Creating virtual environment at $VENV_DIR using $PY_EXEC ..."
            "$PY_EXEC" -m venv "$VENV_DIR"
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
# Generated environment configuration for HW3 cluster execution ($(date))
# Source this file to configure environment: source cluster_env.sh

export REPO_ROOT="$REPO_ROOT"
export PROJECT_ROOT="$REPO_ROOT"
export PYTHONPATH="\$REPO_ROOT:\${PYTHONPATH:-}"

EOF

if [ -n "$LOADED_PY_MODULE" ]; then
    echo "# Cluster Python Module" >> "$ENV_FILE"
    echo "module load $LOADED_PY_MODULE 2>/dev/null || true" >> "$ENV_FILE"
fi
if [ -n "$LOADED_GCC_MODULE" ]; then
    echo "# Cluster GCC Module" >> "$ENV_FILE"
    echo "module load $LOADED_GCC_MODULE 2>/dev/null || true" >> "$ENV_FILE"
fi

cat <<EOF >> "$ENV_FILE"

# Python executable
if [ -f "$VENV_DIR/bin/activate" ]; then
    source "$VENV_DIR/bin/activate"
    export PYTHON="$VENV_DIR/bin/python3"
else
    export PYTHON="$PY_EXEC"
    export PATH="\$(dirname "$PY_EXEC"):\$PATH"
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
