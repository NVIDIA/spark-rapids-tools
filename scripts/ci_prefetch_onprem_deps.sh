#!/bin/bash

# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Prefetch heavy on-prem dependency artifacts (Spark archives, Hadoop jars) used by unit tests
# Usage: scripts/ci_prefetch_onprem_deps.sh [CACHE_DIR] [CONFIG_FILE]
# Defaults:
#   CACHE_DIR   -> $RAPIDS_USER_TOOLS_CACHE_FOLDER or ./.cache/spark_rapids_user_tools_cache
#   CONFIG_FILE -> user_tools/src/spark_rapids_pytools/resources/onprem-configs.json

set -euo pipefail

CACHE_DIR="${1:-${RAPIDS_USER_TOOLS_CACHE_FOLDER:-}}"
CONFIG_FILE="${2:-user_tools/src/spark_rapids_pytools/resources/onprem-configs.json}"

if [[ -z "${CACHE_DIR}" ]]; then
  CACHE_DIR="$(pwd)/.cache/spark_rapids_user_tools_cache"
fi

mkdir -p "${CACHE_DIR}"

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "Config file not found: ${CONFIG_FILE}" >&2
  exit 1
fi

echo "Prefetching on-prem dependencies into: ${CACHE_DIR}"
echo "Using config: ${CONFIG_FILE}"

# Use Python to parse the JSON to avoid installing additional tools
readarray -t URIS < <(python - "$CONFIG_FILE" <<'PY'
import json, sys
cfg_path = sys.argv[1]
with open(cfg_path, 'r', encoding='utf-8') as f:
    data = json.load(f)
uris = []
deps = data.get('dependencies', {}).get('deployMode', {}).get('LOCAL', {})
for ver, items in deps.items():
    if isinstance(items, list):
        for it in items:
            uri = it.get('uri')
            if uri:
                uris.append(uri)
for u in uris:
    print(u)
PY
)

echo "Found ${#URIS[@]} URIs to prefetch"

for url in "${URIS[@]}"; do
  fname="$(basename "${url}")"
  dest="${CACHE_DIR}/${fname}"
  if [[ -f "${dest}" ]]; then
    echo "Already cached: ${fname}"
    continue
  fi
  echo "Downloading: ${url} -> ${dest}"
  curl -L --retry 3 --retry-delay 5 -o "${dest}" "${url}"
done

echo "Prefetch complete. Files in ${CACHE_DIR}:"
ls -lh "${CACHE_DIR}" || true
