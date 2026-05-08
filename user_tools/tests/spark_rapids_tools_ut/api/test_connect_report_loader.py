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

"""Smoke tests for Connect report discovery through YAML catalogs."""

import os
import shutil
import tempfile
import unittest

from spark_rapids_tools.api_v1 import ProfCore, QualCore


class TestConnectReportLoader(unittest.TestCase):
    """Verifies connectReport is discoverable from prof/qual result handlers."""

    sample_app_id = 'application_1234567890_0001'

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.prof_output = os.path.join(self.temp_dir, 'rapids_4_spark_profile')
        self.qual_output = os.path.join(self.temp_dir, 'qual_core_output')
        os.makedirs(self.prof_output, exist_ok=True)
        os.makedirs(self.qual_output, exist_ok=True)
        self._write_prof_status()
        self._write_qual_status()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_prof_status(self):
        status_csv = os.path.join(self.prof_output, 'profiling_status.csv')
        with open(status_csv, 'w', encoding='utf-8') as fh:
            fh.write('Event Log,Status,App ID,Attempt ID,App Name,Description\n')
            fh.write(f'/path/to/eventlog,SUCCESS,{self.sample_app_id},0,ProfTest,ok\n')

    def _write_qual_status(self):
        status_csv = os.path.join(self.qual_output, 'status.csv')
        with open(status_csv, 'w', encoding='utf-8') as fh:
            fh.write('Event Log,Status,App ID,Attempt ID,App Name,Description\n')
            fh.write(f'/path/to/eventlog,SUCCESS,{self.sample_app_id},0,QualTest,ok\n')

    def test_prof_core_registers_connect_tables(self):
        handler = ProfCore(self.prof_output).handler

        for label in ('connectSessions', 'connectOperations', 'connectStatements'):
            self.assertIn(label, handler.tbl_reader_map)
            self.assertTrue(handler.is_per_app_tbl(label))

        reader = handler.get_reader_by_tbl('connectStatements')
        self.assertIsNotNone(reader)
        self.assertEqual(reader.report_id, 'connectReport')

    def test_qual_core_registers_connect_tables_under_raw_metrics(self):
        handler = QualCore(self.qual_output).handler

        for label in ('connectSessions', 'connectOperations', 'connectStatements'):
            self.assertIn(label, handler.tbl_reader_map)
            self.assertTrue(handler.is_per_app_tbl(label))

        reader = handler.get_reader_by_tbl('connectStatements')
        self.assertIsNotNone(reader)
        self.assertEqual(reader.report_id, 'connectReport')
        self.assertTrue(str(reader.out_path).endswith('/raw_metrics'))
