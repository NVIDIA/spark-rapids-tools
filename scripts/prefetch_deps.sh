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
# Prefetch heavy dependency artifacts (Spark archives, Hadoop/GCS/Azure jars) used by unit tests
# Usage: scripts/ci_prefetch_onprem_deps.sh [CACHE_DIR] [PLATFORMS] [CONFIG_DIR]
#
# Arguments:
#   CACHE_DIR  -> Target folder for cached artifacts
#   PLATFORMS  -> Comma-separated list of platform keys matching <platform>-configs.json
#                  e.g. "onprem,dataproc,databricks_aws,databricks_azure"
#   CONFIG_DIR -> Directory containing the *-configs.json files
#
# Defaults:
#   CACHE_DIR  -> $RAPIDS_USER_TOOLS_CACHE_FOLDER or ./.cache/spark_rapids_user_tools_cache
#   PLATFORMS  -> onprem
#   CONFIG_DIR -> user_tools/src/spark_rapids_pytools/resources

set -euo pipefail

CACHE_DIR="${1:-${RAPIDS_USER_TOOLS_CACHE_FOLDER:-}}"
PLATFORMS_CSV="${2:-onprem}"
CONFIG_DIR="${3:-user_tools/src/spark_rapids_pytools/resources}"

if [[ -z "${CACHE_DIR}" ]]; then
  CACHE_DIR="$(pwd)/.cache/spark_rapids_user_tools_cache"
fi

mkdir -p "${CACHE_DIR}"

if [[ ! -d "${CONFIG_DIR}" ]]; then
  echo "Config directory not found: ${CONFIG_DIR}" >&2
  exit 1
fi

echo "Prefetching dependencies into: ${CACHE_DIR}"
echo "Platforms: ${PLATFORMS_CSV}"
echo "Config directory: ${CONFIG_DIR}"

# Use Python to parse per-platform JSON config(s) and collect URIs from the
# defined structure: dependencies -> deployMode -> LOCAL -> <ver>: [ { uri } ]
readarray -t URIS < <(python - "$PLATFORMS_CSV" "$CONFIG_DIR" <<'PY'
import json, os, sys

platforms_csv = sys.argv[1]
config_dir = sys.argv[2]
platforms = [p.strip() for p in platforms_csv.split(',') if p.strip()]

uris = []
seen = set()

for platform in platforms:
    cfg = os.path.join(config_dir, f"{platform}-configs.json")
    if not os.path.isfile(cfg):
        # Silently skip unknown platform files to keep CI robust
        continue
    try:
        with open(cfg, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        continue
    deps = data.get('dependencies', {}).get('deployMode', {}).get('LOCAL', {})
    if isinstance(deps, dict):
        for items in deps.values():
            if isinstance(items, list):
                for it in items:
                    if isinstance(it, dict):
                        uri = it.get('uri')
                        if isinstance(uri, str) and uri and uri not in seen:
                            seen.add(uri)
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
