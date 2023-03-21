# Copyright (c) 2023, NVIDIA CORPORATION.
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

"""Data validation tool basic program."""

import logging
from typing import Callable
import fire
import pkg_resources

# Setup logging
logger = logging.getLogger('validation')
logger.setLevel(logging.INFO)


class Validation:
    """Data Validation tool basic class."""

    def __init__(self, debug=False):
        if debug:
            logger.setLevel(logging.DEBUG)

        self.summary = {}

    def banner(func: Callable):   # pylint: disable=no-self-argument
        """Banner decorator."""
        def wrapper(self, *args, **kwargs):
            name = func.__name__    # pylint: disable=no-member
            logger.info('*** Running validation function "%s" ***', name)

            result = True
            try:
                func(self, *args, **kwargs)     # pylint: disable=not-callable

            except Exception as exception:    # pylint: disable=broad-except
                logger.error('Error: %s', exception)
                result = False

            if result:
                logger.info('*** Check "%s": PASS ***', name)
            else:
                logger.info('*** Check "%s": FAIL ***', name)

            # Save result into summary
            if name in self.summary:
                self.summary[name] = any([result, self.summary[name]])
            else:
                self.summary[name] = result

        return wrapper

    def run_spark_submit(self, options, capture='all'):
        """Run spark application via spark-submit command."""
        cmd = ['$SPARK_HOME/bin/spark-submit']
        cmd += options
        stdout, stderr = self.run_cmd(cmd, capture=capture)
        return stdout + stderr

    def get_validation_scripts(self, name):
        """Get validation script path by name"""
        return pkg_resources.resource_filename(__name__, 'validation_scripts/' + name)

def main():
    """Main function."""
    fire.Fire(Validation)


if __name__ == '__main__':
    main()