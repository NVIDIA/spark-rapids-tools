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
# Usage: scripts/prefetch_deps.sh [CACHE_DIR] [PLATFORMS] [CONFIG_DIR]
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
# defined structure: dependencies -> deployMode -> LOCAL -> <activeBuildVer>: [ { uri } ]
# If activeBuildVer is not set, the highest available numeric version key is used.
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
    deps_local = data.get('dependencies', {}).get('deployMode', {}).get('LOCAL', {})
    if not isinstance(deps_local, dict):
        continue
    active_ver = deps_local.get('activeBuildVer')
    numeric_versions = [k for k, v in deps_local.items() if isinstance(k, str) and k.isdigit() and isinstance(v, list)]
    if not active_ver or active_ver not in deps_local:
        try:
            active_ver = max(numeric_versions, key=lambda x: int(x)) if numeric_versions else None
        except ValueError:
            active_ver = None
    if active_ver and isinstance(deps_local.get(active_ver), list):
        for it in deps_local[active_ver]:
            if isinstance(it, dict):
                uri = it.get('uri')
                if isinstance(uri, str) and uri and uri not in seen:
                    seen.add(uri)
                    uris.append(uri)

for u in uris:
    print(u)
PY
)

# Add Hadoop tarball to prefetch list for HDFS E2E setup.
# Use archive URL in repo; at download time we try dlcdn first,
# and if unavailable, fall back to this archive URL. Use
# E2E_TEST_HADOOP_VERSION if set, else 3.3.6.
HADOOP_VERSION_HINT="${E2E_TEST_HADOOP_VERSION:-3.3.6}"
HADOOP_TARBALL_URL="https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION_HINT}/hadoop-${HADOOP_VERSION_HINT}.tar.gz"
URIS+=("${HADOOP_TARBALL_URL}")

echo "Prepared ${#URIS[@]} URIs to prefetch"
echo "Includes Hadoop ${HADOOP_VERSION_HINT} tarball for HDFS E2E"

for url in "${URIS[@]}"; do
  fname="$(basename "${url}")"
  dest="${CACHE_DIR}/${fname}"
  if [[ -f "${dest}" ]]; then
    echo "Already cached: ${fname}"
    continue
  fi
  # Optimization for Apache artifacts:
  # - We store stable archive.apache.org URLs in-repo because dlcdn prunes old releases.
  # - At download time, for any archive.apache.org/dist URL, we first try the CDN
  #   (dlcdn.apache.org) for better performance/availability, then fall back to archive.
  # - Implementation detail: swap only the host and keep the exact path so the artifact
  #   remains identical between CDN and archive.
  if [[ "${url}" == https://archive.apache.org/dist/* ]]; then
    # Extract the path portion after ".../dist/" and rebuild the CDN URL with the same path.
    rest_path="${url#https://archive.apache.org/dist/}"
    dlcdn_url="https://dlcdn.apache.org/${rest_path}"
    echo "Trying DLCDN first: ${dlcdn_url} -> ${dest}"
    if curl -fsSL --retry 3 --retry-all-errors --retry-delay 5 -o "${dest}" "${dlcdn_url}"; then
      continue
    fi
  fi
  echo "Downloading from source: ${url} -> ${dest}"
  if curl -fsSL --retry 3 --retry-all-errors --retry-delay 5 -o "${dest}" "${url}"; then
    continue
  fi
  echo "Download failed: ${url}" >&2
  rm -f "${dest}" || true
done

echo "Prefetch complete. Files in ${CACHE_DIR}:"
ls -lh "${CACHE_DIR}" || true
