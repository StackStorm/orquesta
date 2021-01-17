# Copyright 2021 The StackStorm Authors.
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

import argparse
import logging
import os

from orquesta import exceptions as exc
from orquesta import rehearsing


LOG = logging.getLogger(__name__)


def process(base_path, test_spec):
    fixture_path = "%s/%s" % (base_path, test_spec)

    if not os.path.isfile(fixture_path):
        raise exc.WorkflowRehearsalError('The test spec "%s" does not exist.' % fixture_path)

    rehearsal = rehearsing.load_test_spec(fixture_path=test_spec, base_path=base_path)
    LOG.info('The test spec "%s" is successfully loaded.' % fixture_path)

    rehearsal.assert_conducting_sequence()
    LOG.info("Completed running test and the workflow execution matches the test spec.")


def rehearse():
    parser = argparse.ArgumentParser("A utility for testing orquesta workflows.")

    parser.add_argument(
        "-p",
        "--base-path",
        type=str,
        required=True,
        help="The base path where the workflow definition, tests, and files are located.",
    )

    # Set up mutually exclusive group for test spec file or directory.
    test_spec_group = parser.add_mutually_exclusive_group(required=True)

    test_spec_group.add_argument(
        "-f",
        "--test-spec",
        type=str,
        help="The path to the test spec (relative from base path) to run the test.",
    )

    test_spec_group.add_argument(
        "-d",
        "--test-spec-dir",
        type=str,
        help="The path to the test spec directory (relative from base path) to run multiple tests.",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Set the log level to debug.",
    )

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    if not os.path.isdir(args.base_path):
        raise exc.WorkflowRehearsalError('The base path "%s" does not exist.' % args.base_path)

    if not args.test_spec_dir:
        process(args.base_path, args.test_spec)
    else:
        errors = False
        fixture_dir = "%s/%s" % (args.base_path, args.test_spec_dir)

        if not os.path.isdir(fixture_dir):
            raise exc.WorkflowRehearsalError(
                'The test spec directory "%s" does not exist.' % fixture_dir
            )

        LOG.info('Using the test spec directory "%s".', fixture_dir)

        test_specs = [
            "%s/%s" % (args.test_spec_dir, entry)
            for entry in os.listdir(fixture_dir)
            if os.path.isfile("%s/%s" % (fixture_dir, entry))
        ]

        LOG.info("Identified the following test specs in the directory: %s", ", ".join(test_specs))

        for test_spec in test_specs:
            try:
                process(args.base_path, test_spec)
            except Exception as e:
                LOG.error(str(e))
                errors = True

        if errors:
            raise exc.WorkflowRehearsalError(
                "There are errors processing test specs. Please review details above."
            )
