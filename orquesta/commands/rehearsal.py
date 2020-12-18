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

import argparse
import os

from orquesta import exceptions as exc
from orquesta import rehearsing


def rehearse():
    parser = argparse.ArgumentParser("A utility for testing orquesta workflows.")

    parser.add_argument(
        "-p",
        "--base-path",
        type=str,
        required=True,
        help="The base path where the workflow definition, tests, and files are located.",
    )

    parser.add_argument(
        "-t",
        "--test-spec",
        type=str,
        required=True,
        help="The path to the test spec (relative from base path) to run the test.",
    )

    args = parser.parse_args()

    if not os.path.isdir(args.base_path):
        raise exc.WorkflowRehearsalError('The base path "%s" does not exist.' % args.base_path)

    fixture_path = "%s/%s" % (args.base_path, args.test_spec)

    if not os.path.isfile(fixture_path):
        raise exc.WorkflowRehearsalError('The test spec "%s" does not exist.' % fixture_path)

    rehearsal = rehearsing.load_test_spec(fixture_path=args.test_spec, base_path=args.base_path)
    print('The test spec "%s" is successfully loaded and no error is found.' % fixture_path)

    rehearsal.assert_conducting_sequence()
    print("Completed running test and the workflow execution matches the test spec.")
