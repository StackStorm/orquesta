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

import copy
import unittest

from orquesta.tests.unit.specs import base as specs


class SpecTest(unittest.TestCase):

    def setUp(self):
        super(SpecTest, self).setUp()
        self.maxDiff = None

    def test_spec_serialization(self):
        spec = {
            'name': 'mock',
            'version': '1.0',
            'description': 'This is a mock spec.',
            'inputs': [
                'x',
                {'y': 'polo'}
            ],
            'vars': {
                'var1': 'foobar',
                'var2': '<% ctx().x %>',
                'var3': '<% ctx().y %>'
            },
            'attr1': 'foobar',
            'attr1-1': 'fubar',
            'attr1_2': 'foosball',
            'attr2': {
                'macro': 'polo'
            },
            'attr3': [
                '<% ctx().var1 %>'
            ],
            'attr4': [
                {'open': 'sesame'},
                {'sesame': 'open'}
            ],
            'attr5': {
                'attr1': {
                    'attr1': '<% ctx().var2 %> <% ctx().var3 %>'
                }
            },
            'attr6': {
                'attr1': {
                    'attr1': {
                        'attr1': 'wunderbar'
                    }
                }
            },
            'attr7': [
                {
                    'attr1': {
                        'attr1': 'wunderbar'
                    }
                },
                {
                    'attr1': {
                        'attr1': 'wonderful'
                    }
                }
            ]
        }

        spec_obj_1 = specs.MockSpec(spec)

        expected_data = {
            'catalog': spec_obj_1.get_catalog(),
            'version': spec_obj_1.get_version(),
            'name': spec_obj_1.name,
            'spec': spec_obj_1.spec
        }

        spec_obj_1_json = spec_obj_1.serialize()

        self.assertDictEqual(spec_obj_1_json, expected_data)

        spec_obj_2 = specs.MockSpec.deserialize(spec_obj_1_json)

        self.assertEqual(spec_obj_2.name, spec_obj_1.name)
        self.assertEqual(spec_obj_2.member, spec_obj_1.member)
        self.assertDictEqual(spec_obj_2.spec, spec_obj_1.spec)

    def test_spec_deserialization_errors(self):
        spec = {
            'name': 'mock',
            'version': '1.0',
            'description': 'This is a mock spec.',
            'inputs': [
                'x',
                {'y': 'polo'}
            ],
            'vars': {
                'var1': 'foobar',
                'var2': '<% ctx().x %>',
                'var3': '<% ctx().y %>'
            },
            'attr1': 'foobar',
            'attr1-1': 'fubar',
            'attr1_2': 'foosball',
            'attr2': {
                'macro': 'polo'
            },
            'attr3': [
                '<% ctx().var1 %>'
            ],
            'attr4': [
                {'open': 'sesame'},
                {'sesame': 'open'}
            ],
            'attr5': {
                'attr1': {
                    'attr1': '<% ctx().var2 %> <% ctx().var3 %>'
                }
            },
            'attr6': {
                'attr1': {
                    'attr1': {
                        'attr1': 'wunderbar'
                    }
                }
            },
            'attr7': [
                {
                    'attr1': {
                        'attr1': 'wunderbar'
                    }
                },
                {
                    'attr1': {
                        'attr1': 'wonderful'
                    }
                }
            ]
        }

        spec_obj = specs.MockSpec(spec)

        spec_obj_json = spec_obj.serialize()

        # Test missing catalog information.
        test_json = copy.deepcopy(spec_obj_json)
        test_json.pop('catalog')

        self.assertRaises(ValueError, specs.MockSpec.deserialize, test_json)

        # Test missing version information.
        test_json = copy.deepcopy(spec_obj_json)
        test_json.pop('version')

        self.assertRaises(ValueError, specs.MockSpec.deserialize, test_json)

        # Test mismatch version information.
        test_json = copy.deepcopy(spec_obj_json)
        test_json['version'] = '99.99'

        self.assertRaises(ValueError, specs.MockSpec.deserialize, test_json)

    def test_mapping_spec_serialization(self):
        spec = {
            'attr1': {
                'attr1': {
                    'attr1': 'wunderbar'
                }
            }
        }

        spec_obj_1 = specs.MockMappingSpec(spec, member=True)

        expected_data = {
            'catalog': spec_obj_1.get_catalog(),
            'version': spec_obj_1.get_version(),
            'member': spec_obj_1.member,
            'spec': spec_obj_1.spec
        }

        spec_obj_1_json = spec_obj_1.serialize()

        self.assertDictEqual(spec_obj_1_json, expected_data)

        spec_obj_2 = specs.MockMappingSpec.deserialize(spec_obj_1_json)

        self.assertEqual(spec_obj_2.member, spec_obj_1.member)
        self.assertDictEqual(spec_obj_2.spec, spec_obj_1.spec)

    def test_sequence_spec_serialization(self):
        spec = [
            {
                'attr1': {
                    'attr1': 'wunderbar'
                }
            },
            {
                'attr1': {
                    'attr1': 'wonderful'
                }
            }
        ]

        spec_obj_1 = specs.MockSequenceSpec(spec, member=True)

        expected_data = {
            'catalog': spec_obj_1.get_catalog(),
            'version': spec_obj_1.get_version(),
            'member': spec_obj_1.member,
            'spec': spec_obj_1.spec
        }

        spec_obj_1_json = spec_obj_1.serialize()

        self.assertDictEqual(spec_obj_1_json, expected_data)

        spec_obj_2 = specs.MockSequenceSpec.deserialize(spec_obj_1_json)

        self.assertEqual(spec_obj_2.member, spec_obj_1.member)
        self.assertListEqual(spec_obj_2.spec, spec_obj_1.spec)
