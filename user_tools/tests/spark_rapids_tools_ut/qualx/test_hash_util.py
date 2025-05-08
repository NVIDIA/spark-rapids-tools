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

"""Test hash_util module"""
from spark_rapids_tools.tools.qualx.hash_util import (
    split_fields,
    strip_ids,
    path_match,
    normalize_plan,
    hash_plan
)
from ..conftest import SparkRapidsToolsUT


class TestPlanHash(SparkRapidsToolsUT):
    """Test cases for plan_hash.py functions."""

    def test_split_fields(self):
        """Test the split_fields function."""
        # Test with simple comma-separated values
        assert split_fields('a, b, c') == ['a', 'b', 'c']

        # Test with empty string
        assert not split_fields('')

        # Test with nested parentheses
        assert split_fields('a, b(x), c(x,y), d(x,e(y))') == ['a', 'b(x)', 'c(x,y)', 'd(x,e(y))']

        # Test with CASE statements
        case_expr = 'CASE WHEN x IN (1,2) THEN y ELSE z END'
        assert split_fields(f'a, {case_expr}, b') == ['a', case_expr, 'b']

        # Test with normalize=True
        assert split_fields(f'a, {case_expr}, b', normalize=True) == ['a', 'case_statement', 'b']

        # Test with complex nested expressions
        complex_expr = 'a, b(x), c(x,y), d(x,e(y)), CASE WHEN x IN (1,2) THEN y ELSE z END'
        assert split_fields(complex_expr, normalize=True) == ['a', 'b(x)', 'c(x,y)', 'd(x,e(y))', 'case_statement']

    def test_strip_ids(self):
        """Test the strip_ids function."""
        # Test with simple ID
        assert strip_ids('foo#1234L') == 'foo'

        # Test with multiple IDs
        assert strip_ids('foo#1234L#5678L') == 'foo'

        # Test with no ID
        assert strip_ids('foo') == 'foo'

        # Test with empty string
        assert strip_ids('') == ''

    def test_path_match(self):
        """Test the path_match function."""
        # Create a simple node tree
        node = {
            'nodeName': 'A',
            'children': [
                {
                    'nodeName': 'B',
                    'children': [
                        {
                            'nodeName': 'C',
                            'children': []
                        }
                    ]
                }
            ]
        }

        # Test exact match
        assert path_match(node, ['A'])

        # Test path match
        assert path_match(node, ['A', 'B'])
        assert path_match(node, ['A', 'B', 'C'])

        # Test non-match
        assert not path_match(node, ['A', 'C'])
        assert not path_match(node, ['B'])

        # Test with empty path
        assert not path_match(node, [])

    def test_normalize_plan(self):
        """Test the normalize_plan function."""
        # Create a simple plan with nodes to be removed
        plan = {
            'nodeName': 'AdaptiveSparkPlan',
            'children': [
                {
                    'nodeName': 'Exchange',
                    'children': [
                        {
                            'nodeName': 'GpuFilter',
                            'children': []
                        }
                    ]
                }
            ]
        }

        # Normalize the plan
        normalized = normalize_plan(plan)

        # Check that AdaptiveSparkPlan and Exchange were removed
        assert normalized['nodeName'] == 'Filter'
        assert len(normalized['children']) == 0

        # Test with GpuTopN/GpuTopN path
        plan = {
            'nodeName': 'GpuTopN',
            'children': [
                {
                    'nodeName': 'GpuTopN',
                    'children': [
                        {
                            'nodeName': 'X',
                            'children': []
                        }
                    ]
                }
            ]
        }

        normalized = normalize_plan(plan)
        assert normalized['nodeName'] == 'TakeOrderedAndProject'
        assert len(normalized['children']) == 1
        assert normalized['children'][0]['nodeName'] == 'X'

        # Test with Project/Filter path
        plan = {
            'nodeName': 'Project',
            'children': [
                {
                    'nodeName': 'Filter',
                    'children': [
                        {
                            'nodeName': 'X',
                            'children': []
                        }
                    ]
                }
            ]
        }

        normalized = normalize_plan(plan)
        assert normalized['nodeName'] == 'Project'
        assert len(normalized['children']) == 1
        assert normalized['children'][0]['nodeName'] == 'X'

        # Test with UnionWithLocalData/Union path
        plan = {
            'nodeName': 'UnionWithLocalData',
            'children': [
                {
                    'nodeName': 'Union',
                    'children': [
                        {
                            'nodeName': 'X',
                            'children': []
                        }
                    ]
                }
            ]
        }

        normalized = normalize_plan(plan)
        assert normalized['nodeName'] == 'X'
        assert len(normalized['children']) == 0

        # Test with SubqueryAdaptiveBroadcast
        plan = {
            'nodeName': 'X',
            'children': [
                {
                    'nodeName': 'SubqueryAdaptiveBroadcast',
                    'children': []
                }
            ]
        }

        normalized = normalize_plan(plan)
        assert normalized['nodeName'] == 'X'
        assert len(normalized['children']) == 0

    def test_hash_plan(self):
        """Test the hash_plan function."""
        # Create a simple plan
        plan = {
            'nodeName': 'Project',
            'simpleString': '[a#1, b#2, c#3]',
            'children': [
                {
                    'nodeName': 'Filter',
                    'children': []
                }
            ]
        }

        # Generate hash
        hash_value1 = hash_plan(plan)

        # Check that hash is an integer
        assert isinstance(hash_value1, int)

        # Test with different plans
        plan2 = {
            'nodeName': 'Project',
            'simpleString': '[a#1, b#2, d#4]',  # Different field
            'children': [
                {
                    'nodeName': 'Filter',
                    'children': []
                }
            ]
        }

        hash_value2 = hash_plan(plan2)

        # Different plans should have different hashes
        assert hash_value1 != hash_value2

        # Test with identical plans
        plan3 = {
            'nodeName': 'Project',
            'simpleString': '[a#4, b#5, c#6]',
            'children': [
                {
                    'nodeName': 'Filter',
                    'children': []
                }
            ]
        }

        hash_value3 = hash_plan(plan3)

        # Identical plans should have the same hash
        assert hash_value1 == hash_value3
