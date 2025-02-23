# Copyright 2021 The StackStorm Authors.
# Copyright 2019 Extreme Networks, Inc.
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

import json
import logging
import os
import yaml


LOG = logging.getLogger(__name__)

FIXTURE_TYPES = ["workflows"]

FIXTURE_EXTS = {".json": json.load, ".yml": yaml.safe_load, ".yaml": yaml.safe_load}


def get_fixtures_base_path():
    return os.path.dirname(__file__)


def get_workflow_fixtures_base_path():
    return os.path.join(get_fixtures_base_path(), "workflows")


def get_rehearsal_fixtures_base_path():
    return os.path.join(get_fixtures_base_path(), "rehearsals")


def get_fixture_content(fixture_file_name, fixture_type, raw=False):
    if fixture_type not in FIXTURE_TYPES:
        raise Exception("Unsupported fixture type of %s" % fixture_type)

    fixture_type_path = os.path.join(get_fixtures_base_path(), fixture_type)
    file_path = os.path.join(fixture_type_path, fixture_file_name)
    file_name, file_ext = os.path.splitext(file_path)

    if file_ext not in FIXTURE_EXTS.keys():
        raise Exception("Unsupported fixture file ext type of %s." % file_ext)

    with open(file_path, "r") as fd:
        return FIXTURE_EXTS[file_ext](fd) if not raw else fd.read()
