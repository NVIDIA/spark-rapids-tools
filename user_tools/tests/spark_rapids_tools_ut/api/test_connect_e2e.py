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

"""Golden roundtrip checks for Spark Connect profiler output."""

import shutil
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from spark_rapids_tools.api_v1 import ProfCore


class TestConnectE2E(unittest.TestCase):
    """Verifies a committed Connect profiler output tree is readable end to end."""

    sample_app_id = 'local-connect-e2e'
    expected_statement = 'common { plan_id: 0 } range { start: 0 end: 100 step: 1 }\n'

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        fixture_root = Path(__file__).resolve().parents[1] / 'resources' / 'connect_e2e'
        self.prof_output = Path(self.temp_dir) / 'rapids_4_spark_profile'
        shutil.copytree(fixture_root / 'rapids_4_spark_profile', self.prof_output)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_connect_operations_roundtrip_via_csv_and_api(self):
        handler = ProfCore(str(self.prof_output))
        api_res = handler.csv('connectOperations').app(self.sample_app_id).load()
        self.assertTrue(api_res.success)

        csv_path = self.prof_output / self.sample_app_id / 'connect_operations.csv'
        raw_df = pd.read_csv(csv_path)
        api_df = api_res.data

        self.assertEqual(list(raw_df['operationId']), ['op-bbb-222', 'op-ccc-333'])
        self.assertEqual(list(api_df['operationId'].astype(str)), list(raw_df['operationId']))
        self.assertEqual(api_df.loc[api_df['operationId'] == 'op-bbb-222', 'sqlIds'].iat[0], '42')
        self.assertEqual(api_df.loc[api_df['operationId'] == 'op-bbb-222', 'jobIds'].iat[0], '7')
        self.assertEqual(api_df.loc[api_df['operationId'] == 'op-ccc-333', 'status'].iat[0], 'FAILED')

        sessions_res = handler.csv('connectSessions').app(self.sample_app_id).load()
        self.assertTrue(sessions_res.success)
        self.assertEqual(int(sessions_res.data['operationCount'].iat[0]), 2)

    def test_connect_statement_sidecar_roundtrip(self):
        handler = ProfCore(str(self.prof_output))

        stmt_dir = handler.get_connect_statements_dir(self.sample_app_id)
        self.assertIsNotNone(stmt_dir)
        self.assertEqual(stmt_dir.base_name(), 'connect_statements')
        self.assertEqual(handler.list_connect_statement_ops(self.sample_app_id), ['op-bbb-222'])
        self.assertEqual(
            handler.load_connect_statement(self.sample_app_id, 'op-bbb-222'),
            self.expected_statement)
        self.assertIsNone(handler.load_connect_statement(self.sample_app_id, 'op-ccc-333'))
