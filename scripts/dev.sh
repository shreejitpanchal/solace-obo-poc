#!/usr/bin/env bash
# Dev tasks. The only place that knows how to build/test/lint/scan this repo.
# CI calls task names only. Keep dev.ps1 behaviourally identical.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
DIST="$REPO_ROOT/dist"
BUILD_DIR="$REPO_ROOT/build"
mkdir -p "$LOG_DIR"
cd "$REPO_ROOT"

export NO_COLOR=1
export PIP_PROGRESS_BAR=off
export PIP_DISABLE_PIP_VERSION_CHECK=1

# --- output helpers ---------------------------------------------------------
c() { printf '\033[%sm%s\033[0m\n' "$1" "$2"; }
step() { c '1;36' "==> $*"; }
ok()   { c '1;32' "ok: $*"; }
warn() { c '1;33' "warn: $*"; }
die()  { c '1;31' "error: $*"; exit 1; }

now() { date +%Y-%m-%dT%H:%M:%S%z; }

# Truncate this task's log with a header, then everything tees onto it.
log_begin() {
  printf '=== %s | %s ===\n' "$(now)" "$1" > "$LOG_DIR/$1.log"
}

# finish <task> <exit-code> <elapsed-seconds>
finish() {
  local task=$1 code=$2 secs=$3 status
  if [ "$code" -eq 0 ]; then status=OK; else status="FAILED (exit $code)"; fi
  printf '%s | %s | %ss | %s\n' "$(now)" "$task" "$secs" "$status" \
    | tee -a "$LOG_DIR/$task.log"
}

# Strip ANSI/CSI so logs stay readable plain text.
strip_csi() { sed -E $'s/\x1b\\[[0-9;?]*[a-zA-Z]//g'; }

# run <task> <cmd...> -- tees combined output, returns the command's code.
run() {
  local task=$1; shift
  "$@" 2>&1 | strip_csi | tee -a "$LOG_DIR/$task.log"
  return "${PIPESTATUS[0]}"
}

# --- toolchain --------------------------------------------------------------
# uv owns the interpreter (.python-version), the venv and the lockfile. CI calls
# this script with no setup step, so bootstrap uv here when a runner lacks it.
PY="$(command -v python3 || command -v python || true)"
UV="uv"
ensure_uv() {
  command -v uv >/dev/null 2>&1 && return 0
  [ -n "$PY" ] || { warn "neither uv nor python on PATH; install uv: https://docs.astral.sh/uv/"; return 1; }
  warn "uv not on PATH; installing it with pip"
  "$PY" -m pip install --quiet uv || { warn "pip install uv failed; install uv manually: https://docs.astral.sh/uv/"; return 1; }
  if command -v uv >/dev/null 2>&1; then UV="uv"; else UV="$PY -m uv"; fi
}

# --- target resolution ------------------------------------------------------
# CI sets TARGET_OS/TARGET_ARCH; unset means host. PyInstaller cannot
# cross-compile, so build refuses any target that is not the host.
host_os()   { case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) echo windows;; *) uname -s | tr '[:upper:]' '[:lower:]';; esac; }
host_arch() { case "$(uname -m)" in x86_64|amd64) echo amd64;; aarch64|arm64) echo arm64;; *) uname -m;; esac; }
T_OS="${TARGET_OS:-$(host_os)}"
T_ARCH="${TARGET_ARCH:-$(host_arch)}"

# One binary per service in the POC; names carry the target so the release
# job's merge-multiple never collides.
COMPONENTS="issuer publisher validator recipient profiler news_consumer"

# --- tasks ------------------------------------------------------------------
task_build() {
  ensure_uv || return 1
  if [ "$T_OS" != "$(host_os)" ] || [ "$T_ARCH" != "$(host_arch)" ]; then
    warn "cannot build $T_OS/$T_ARCH on a $(host_os)/$(host_arch) host: PyInstaller does not cross-compile."
    warn "fix: set BUILD_TARGETS to targets this runner can build, or add a runner for $T_OS/$T_ARCH."
    return 1
  fi
  mkdir -p "$DIST" "$BUILD_DIR"
  run build $UV sync || return $?
  local comp
  for comp in $COMPONENTS; do
    # --name gets .exe appended automatically on Windows. Module underscores
    # become dashes so the binary matches its console script (obo-news-consumer).
    run build $UV run pyinstaller --noconfirm --clean --onefile --log-level WARN \
      --name "obo-${comp//_/-}-$T_OS-$T_ARCH" \
      --distpath "$DIST" --workpath "$BUILD_DIR" --specpath "$BUILD_DIR" \
      --paths "$REPO_ROOT/src" --collect-all solace \
      "$REPO_ROOT/src/solace_obo_poc/$comp.py" || return $?
  done
  ls -1 "$DIST" | tee -a "$LOG_DIR/build.log"
}

task_vet() {
  ensure_uv || return 1
  run vet $UV run ruff check . || return $?
  run vet $UV run ruff format --check .
}

task_test() {
  ensure_uv || return 1
  run test $UV run pytest -q
}

task_cov() {
  ensure_uv || return 1
  run cov $UV run pytest -q --cov --cov-branch \
    --cov-report=term-missing --cov-report=html:coverage_html --cov-report=xml:coverage.xml || return $?
  # Print the total on its own line so the log is greppable. The previous
  # total in logs/cov.log is the floor (local only; CI is a fresh checkout).
  printf 'coverage total: %s%%\n' "$($UV run coverage report --format=total)" | tee -a "$LOG_DIR/cov.log"
}

# build_image <log-task> -- logged under the calling task so a standalone
# `scan` never appends to a stale image.log.
build_image() {
  run "$1" docker build --progress=plain -t "$(basename "$REPO_ROOT"):dev" .
}

task_image() {
  [ -f "$REPO_ROOT/Dockerfile" ] || { warn "no Dockerfile; skipping image"; return 0; }
  build_image image
}

# One task, every applicable check. FATAL on fixable CVEs.
task_scan() {
  ensure_uv || return 1
  local code=0 req="$BUILD_DIR/requirements-audit.txt" report="$LOG_DIR/pip-audit.json"
  mkdir -p "$BUILD_DIR"
  rm -f "$report"
  # Audit the fully resolved lock (all groups, project itself excluded) rather
  # than the venv: uv venvs carry no pip, which pip-audit needs for that mode.
  run scan $UV export --format requirements-txt --no-hashes --no-emit-project --all-groups -o "$req" || return $?
  # pip-audit exits non-zero on ANY finding; audit_gate.py applies the real
  # rule -- fixable is fatal, unfixable warns -- identically in both scripts.
  run scan $UV run pip-audit -r "$req" --no-deps --progress-spinner off --format json -o "$report"
  [ -f "$report" ] || { warn "pip-audit produced no report; see logs/scan.log"; return 1; }
  run scan $UV run python "$SCRIPT_DIR/audit_gate.py" "$report" || code=$?

  # Image half: only when this project ships an image, never against a stale one.
  if [ -f "$REPO_ROOT/Dockerfile" ]; then
    build_image scan || return $?
    # --pull=always matters more than the tag: without it docker reuses
    # whatever :latest resolved to months ago -- unpinned AND stale.
    run scan docker run --rm --pull=always -e NO_COLOR=1 aquasec/trivy:latest image \
      --quiet --exit-code 1 --severity HIGH,CRITICAL --ignore-unfixed \
      "$(basename "$REPO_ROOT"):dev" || code=$?
  fi
  return $code
}

task_up()   { run up   docker compose up -d; }
task_down() { run down docker compose down; }

# Local only: the graph is a developer artifact, not a CI output.
task_graphify() {
  [ -n "${CI:-}" ] && { warn "graphify is local-only; skipping in CI"; return 0; }
  command -v graphify >/dev/null || { warn "graphify not on PATH; skipping"; return 0; }
  [ -f "$REPO_ROOT/graphify-out/graph.json" ] || { warn "no graph yet; run 'graphify .' once to create it; skipping"; return 0; }
  run graphify graphify update .
}

# --- dispatch ---------------------------------------------------------------
ALL="build vet test"
FULL="build vet test cov image scan graphify"

usage() {
  cat <<USAGE
usage: $(basename "$0") <task>...

  build vet test cov scan image up down graphify
  all   = $ALL            (what CI runs, as: all scan)
  full  = $FULL           (pre-tag sweep)
USAGE
}

expand() {
  case "$1" in
    all)  echo "$ALL" ;;
    full) echo "$FULL" ;;
    *)    echo "$1" ;;
  esac
}

[ $# -eq 0 ] && { usage; exit 0; }
case "${1:-}" in -h|--help|help) usage; exit 0 ;; esac

TASKS=""
for a in "$@"; do TASKS="$TASKS $(expand "$a")"; done

FAILED=0
for task in $TASKS; do
  type "task_$task" >/dev/null 2>&1 || die "unknown task: $task"
  step "$task"
  log_begin "$task"
  start=$SECONDS
  code=0
  "task_$task" || code=$?
  finish "$task" "$code" "$((SECONDS - start))"
  if [ "$code" -ne 0 ]; then
    FAILED=1
    warn "$task failed; stopping"
    break   # build/vet/test/scan are all fatal
  fi
  ok "$task"
done
exit "$FAILED"
