# Copyright (c) 2024, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Module to test the sanitize_for_posix function. """

import unittest

from spark_rapids_tools_distributed.spark_map_task.jar_runner import SparkJarRunner


class TestSanitizeForPosix(unittest.TestCase):
    """ Test class for the sanitize_for_posix function. """

    def test_basic_filename(self):
        result = SparkJarRunner.sanitize_for_posix('my file.txt')
        self.assertEqual('my_file_txt', result)

    def test_special_characters(self):
        result = SparkJarRunner.sanitize_for_posix('my*file/with|special:chars?.txt')
        self.assertEqual('my_file_with_special_chars__txt', result)

    def test_leading_and_trailing_spaces(self):
        result = SparkJarRunner.sanitize_for_posix('  file name with spaces   ')
        self.assertEqual('file_name_with_spaces', result)

    def test_only_special_characters(self):
        result = SparkJarRunner.sanitize_for_posix('/<>|:*?"')
        self.assertEqual('________', result)

    def test_mixed_content(self):
        result = SparkJarRunner.sanitize_for_posix('Project #1/Final-Version (Draft)')
        self.assertEqual('Project__1_Final-Version__Draft_', result)

    def test_too_long_filename(self):
        long_name = 'a' * 300
        expected_output = 'a' * SparkJarRunner.max_file_length
        result = SparkJarRunner.sanitize_for_posix(long_name)
        self.assertEqual(expected_output, result)

    def test_already_clean_filename(self):
        result = SparkJarRunner.sanitize_for_posix('valid_filename-123.txt')
        self.assertEqual('valid_filename-123_txt', result)

    def test_filename_with_slashes(self):
        result = SparkJarRunner.sanitize_for_posix('backup/2024/November')
        self.assertEqual('backup_2024_November', result)

    def test_filename_with_multiple_spaces(self):
        result = SparkJarRunner.sanitize_for_posix('   multiple   spaces  ')
        self.assertEqual('multiple___spaces', result)

    def test_filename_with_trailing_special_chars(self):
        result = SparkJarRunner.sanitize_for_posix('trailing-special-#/')
        self.assertEqual('trailing-special-__', result)

    def test_filename_with_valid_chars_only(self):
        result = SparkJarRunner.sanitize_for_posix('valid-123_filename.txt')
        self.assertEqual('valid-123_filename_txt', result)
