#!/bin/bash

# Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

# Git pre-commit hook to check copyright headers for current year exists for newly added files

readonly NEW_FILES=$(git diff --diff-filter=ACRTU --cached --name-only)
readonly YEAR=$(date +%Y)
INVALID_FILES=()
GREP_EXCLUDE_OPTIONS=()
IFS=$'\n'

EXCLUDE_PATTERNS=(
    "core/src/main/resources/*"
    "core/src/test/resources/*"
    "user_tools/src/spark_rapids_pytools/resources/*"
    "core/docs/*"
    "user_tools/docs/*"
    "user_tools/tests/spark_rapids_tools_ut/resources/*"
    "*.csv"
    "*.zstd"
    "*.gitkeep"
    )

# Create the grep exclude options (--exclude=*csv --exclude=core/src/test/resources/*)
for pattern in "${EXCLUDE_PATTERNS[@]}"; do
    GREP_EXCLUDE_OPTIONS+=("--exclude=$pattern")
done

if [ -n "$NEW_FILES" ]; then
    for f in $NEW_FILES; do
        echo "Checking new file: $f"
        INVALID_FILES+=($(grep -L "${GREP_EXCLUDE_OPTIONS[@]}" "Copyright (c) $YEAR" "$f"))
    done
fi

if [ ${#INVALID_FILES[@]} -gt 0 ]; then
    echo "Found new files with incorrect headers:"
    for f in "${INVALID_FILES[@]}"; do
        echo "    $f"
    done
    exit 1
fi
