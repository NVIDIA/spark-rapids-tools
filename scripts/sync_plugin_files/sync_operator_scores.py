# Copyright (c) 2024, NVIDIA CORPORATION.
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

"""
Description:
    This Python script takes an input text file of operators (separated by \newline),
    appends them to the operator score files under operator_score_dir directory, and
    assigns them an default or user input score.

Dependencies:
    - numpy >= 1.23.3
    - pandas >= 2.0.3

Usage:
    python sync_operator_scores.py new_operators operator_score_dir [--new-score score]
"""

import argparse
import json
import logging
import os

import pandas as pd


def main(argvs):
    """
    Main function of the script.

    Parameters:
    args: Namespace containing the command-line arguments
    """

    new_operators_file = argvs.new_operators_file
    operator_score_dir = argvs.operator_score_dir
    score = argvs.new_score

    new_operators = set()
    with open(new_operators_file, 'r') as f:
        for line in f:
            new_operators.add(line.strip())

    if os.path.exists(operator_score_dir) and os.path.isdir(operator_score_dir):
        for file in os.listdir(operator_score_dir):
            file_path = os.path.join(operator_score_dir, file)
            if "operatorsScore" in file_path and os.path.isfile(file_path):
                operator_file = open(file_path, 'a')
                for operator in new_operators:
                    operator_file.write(f"{operator},{score}\n")
    return


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("new_operators_file", type=str, help="A text file with the new operators.")
    parser.add_argument('operator_score_dir', type=str, help='Path to directory with operator score files.')
    parser.add_argument('--new-score', type=float, help='Score for the new operators.', default='1.0')

    args = parser.parse_args()

    main(args)
