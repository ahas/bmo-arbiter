#!/usr/bin/env bash
# The card arbiter's environment on its node, stood up from what is already there (docs/node-arbiter.md). It is
# run detached with its output in setup.log, as every node setup of this project is, and it says
# `CARD-SETUP-DONE ok` or `CARD-SETUP-DONE failed:<step>` on its last line, which is what the caller
# waits for.
#
# It is idempotent: what is already right is left alone and said so. It installs into the account
# alone — no sudo, no system package, and nothing written to the account's shell profile.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="$PATH:$HOME/.local/bin"
# uv's managed interpreter, rather than the system python, so every node's venv is the same one.
VENV_PYTHON="3.12"
UV_INSTALLER="https://astral.sh/uv/install.sh"

step() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { printf 'CARD-SETUP-DONE failed:%s\n' "$1"; exit 1; }

cd "$here" || fail cd

if command -v uv >/dev/null 2>&1; then
  step "uv already here: $(uv --version)"
else
  step "installing uv into the account"
  curl -LsSf "$UV_INSTALLER" | env UV_NO_MODIFY_PATH=1 sh >/dev/null 2>&1 || fail uv-install
  command -v uv >/dev/null 2>&1 || fail uv-missing-after-install
  step "uv installed: $(uv --version)"
fi

# A venv standing on the system python, or on an interpreter no longer there, is not the venv this
# script makes. It is made again on uv's own.
if [ -d "$here/.venv" ]; then
  base="$("$here/.venv/bin/python" -c 'import sys; print(sys.base_prefix)' 2>/dev/null || true)"
  case "$base" in
    "$(uv python dir)"/*) ;;
    *)
      step "venv stands on ${base:-no interpreter}, not on uv's own python: making it again"
      rm -rf "${here:?}/.venv"
      ;;
  esac
fi

if [ -x "$here/.venv/bin/python" ]; then
  step "venv already here: $("$here/.venv/bin/python" -V 2>&1)"
else
  step "making the venv on python $VENV_PYTHON"
  uv venv --system-certs --managed-python --python "$VENV_PYTHON" "$here/.venv" || fail venv
fi

step "installing what pyproject pins"
VIRTUAL_ENV="$here/.venv" uv pip install --system-certs -r "$here/pyproject.toml" || fail install

step "what stands now: $("$here/.venv/bin/python" -c 'import importlib.metadata as m; print("nvidia-ml-py", m.version("nvidia-ml-py"))' 2>&1)"

printf 'CARD-SETUP-DONE ok\n'
