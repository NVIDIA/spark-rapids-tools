# Copyright (c) 2026, NVIDIA CORPORATION.
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

"""Tests for generic artifact-path lookups and Connect-specific helpers."""

import os
import shutil
import tempfile
import unittest

from spark_rapids_tools.api_v1 import ProfCore, ProfWrapper


class TestConnectHelpers(unittest.TestCase):
    """Verifies listing and reading Connect statement sidecars."""

    sample_app_id = 'application_1234567890_0001'

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.prof_output = os.path.join(self.temp_dir, 'rapids_4_spark_profile')
        self.prof_wrapper_output = os.path.join(self.temp_dir, 'prof_20260422010101_deadbeef')
        self.prof_wrapper_core_output = os.path.join(self.prof_wrapper_output, 'rapids_4_spark_profile')
        self.app_dir = os.path.join(self.prof_output, self.sample_app_id)
        self.statements_dir = os.path.join(self.app_dir, 'connect_statements')
        os.makedirs(self.statements_dir, exist_ok=True)
        os.makedirs(os.path.join(
            self.prof_wrapper_core_output,
            self.sample_app_id,
            'connect_statements'
        ), exist_ok=True)

        for base_dir in (self.prof_output, self.prof_wrapper_core_output):
            app_dir = os.path.join(base_dir, self.sample_app_id)
            statements_dir = os.path.join(app_dir, 'connect_statements')
            with open(os.path.join(base_dir, 'profiling_status.csv'), 'w', encoding='utf-8') as fh:
                fh.write('Event Log,Status,App ID,Attempt ID,App Name,Description\n')
                fh.write(f'/path/to/eventlog,SUCCESS,{self.sample_app_id},0,ProfTest,ok\n')
            with open(os.path.join(statements_dir, 'op-1.txt'), 'w', encoding='utf-8') as fh:
                fh.write('SELECT 1')
            with open(os.path.join(statements_dir, 'op-2.txt'), 'w', encoding='utf-8') as fh:
                fh.write('SELECT 2')
        with open(os.path.join(self.app_dir, 'secret.txt'), 'w', encoding='utf-8') as fh:
            fh.write('SECRET')

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_get_connect_statements_dir_returns_per_app_path(self):
        handler = ProfCore(self.prof_output)
        path = handler.get_connect_statements_dir(self.sample_app_id)
        self.assertIsNotNone(path)
        self.assertEqual(path.base_name(), 'connect_statements')

    def test_get_per_app_table_path_returns_connect_directory_path(self):
        handler = ProfCore(self.prof_output)
        path = handler.get_per_app_table_path('connectStatements', self.sample_app_id)
        self.assertIsNotNone(path)
        self.assertEqual(path.base_name(), 'connect_statements')
        self.assertTrue(str(path).endswith(f'/{self.sample_app_id}/connect_statements'))

    def test_get_table_path_resolves_nested_core_artifacts_from_wrapper(self):
        handler = ProfWrapper(self.prof_wrapper_output)
        status_path = handler.get_table_path('coreCSVStatus')
        stmt_dir = handler.get_per_app_table_path('connectStatements', self.sample_app_id)
        self.assertIsNotNone(status_path)
        self.assertIsNotNone(stmt_dir)
        self.assertTrue(str(status_path).endswith('/rapids_4_spark_profile/profiling_status.csv'))
        self.assertTrue(str(stmt_dir).endswith(
            f'/rapids_4_spark_profile/{self.sample_app_id}/connect_statements'))

    def test_list_connect_statement_ops_returns_sorted_operation_ids(self):
        handler = ProfCore(self.prof_output)
        ops = handler.list_connect_statement_ops(self.sample_app_id)
        self.assertEqual(ops, ['op-1', 'op-2'])

    def test_load_connect_statement_reads_file(self):
        handler = ProfCore(self.prof_output)
        text = handler.load_connect_statement(self.sample_app_id, 'op-2')
        self.assertEqual(text, 'SELECT 2')

    def test_load_connect_statement_missing_returns_none(self):
        handler = ProfCore(self.prof_output)
        self.assertIsNone(handler.load_connect_statement(self.sample_app_id, 'missing-op'))
        self.assertIsNone(handler.load_connect_statement('missing-app', 'op-1'))

    def test_load_connect_statement_sanitizes_operation_id_before_reading(self):
        handler = ProfCore(self.prof_output)
        self.assertIsNone(handler.load_connect_statement(self.sample_app_id, '../secret'))
