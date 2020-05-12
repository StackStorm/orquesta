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

from orquesta.tests.unit import base as test_base
from orquesta.utils import jsonify as json_util


MOCK_LIST_DATA = {
    "vms": [
        {"name": "vmweb1", "region": "us-east", "role": "web"},
        {"name": "vmdb1", "region": "us-east", "role": "db"},
        {"name": "vmweb2", "region": "us-west", "role": "web"},
        {"name": "vmdb2", "region": "us-west", "role": "db"},
    ]
}

MOCK_DICT_DATA = {
    "vms": {
        "vmdb1": {"region": "us-east", "role": "db", "name": "vmdb1"},
        "vmdb2": {"region": "us-west", "role": "db", "name": "vmdb2"},
        "vmweb1": {"region": "us-east", "role": "web", "name": "vmweb1"},
        "vmweb2": {"region": "us-west", "role": "web", "name": "vmweb2"},
    }
}


class YAQLContextQueryTest(test_base.ExpressionEvaluatorTest):
    @classmethod
    def setUpClass(cls):
        cls.language = "yaql"
        super(YAQLContextQueryTest, cls).setUpClass()

    def test_ctx_list_query(self):
        expected_result = [i["name"] for i in MOCK_LIST_DATA["vms"]]

        expr = "<% ctx(vms).select($.name) %>"
        self.assertListEqual([], self.evaluator.validate(expr))
        self.assertListEqual(self.evaluator.evaluate(expr, MOCK_LIST_DATA), expected_result)

        expr = "<% ctx().vms.select($.name) %>"
        self.assertListEqual([], self.evaluator.validate(expr))
        self.assertListEqual(self.evaluator.evaluate(expr, MOCK_LIST_DATA), expected_result)

    def test_ctx_list_indexing(self):
        expected_result = {"name": "vmweb1", "region": "us-east", "role": "web"}

        expr = "<% ctx(vms)[0] %>"
        self.assertListEqual([], self.evaluator.validate(expr))
        self.assertDictEqual(self.evaluator.evaluate(expr, MOCK_LIST_DATA), expected_result)

        expr = "<% ctx().vms[0] %>"
        self.assertListEqual([], self.evaluator.validate(expr))
        self.assertDictEqual(self.evaluator.evaluate(expr, MOCK_LIST_DATA), expected_result)

    def test_ctx_list_to_dict_transform(self):
        expected_result = json_util.deepcopy(MOCK_DICT_DATA)

        expr = "<% dict(vms=>dict(ctx(vms).select([$.name, $]))) %>"
        self.assertListEqual([], self.evaluator.validate(expr))
        self.assertDictEqual(self.evaluator.evaluate(expr, MOCK_LIST_DATA), expected_result)

        expr = "<% dict(vms=>dict(ctx().vms.select([$.name, $]))) %>"
        self.assertListEqual([], self.evaluator.validate(expr))
        self.assertDictEqual(self.evaluator.evaluate(expr, MOCK_LIST_DATA), expected_result)

    def test_ctx_dict_get(self):
        expected_result = {"name": "vmweb1", "region": "us-east", "role": "web"}

        expr = "<% ctx(vms).get(vmweb1) %>"
        self.assertListEqual([], self.evaluator.validate(expr))
        self.assertDictEqual(self.evaluator.evaluate(expr, MOCK_DICT_DATA), expected_result)

        expr = "<% ctx().vms.get(vmweb1) %>"
        self.assertListEqual([], self.evaluator.validate(expr))
        self.assertDictEqual(self.evaluator.evaluate(expr, MOCK_DICT_DATA), expected_result)

    def test_ctx_dict_indexing(self):
        expected_result = {"name": "vmweb1", "region": "us-east", "role": "web"}

        expr = "<% ctx(vms)[vmweb1] %>"
        self.assertListEqual([], self.evaluator.validate(expr))
        self.assertDictEqual(self.evaluator.evaluate(expr, MOCK_DICT_DATA), expected_result)

        expr = "<% ctx().vms[vmweb1] %>"
        self.assertListEqual([], self.evaluator.validate(expr))
        self.assertDictEqual(self.evaluator.evaluate(expr, MOCK_DICT_DATA), expected_result)
