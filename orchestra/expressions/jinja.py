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

from functools import partial
import inspect
import logging
import re
import six

import jinja2

from orchestra import exceptions as exc
from orchestra.expressions import base
from orchestra.expressions.functions import base as functions


LOG = logging.getLogger(__name__)


def register_functions(env):
    catalog = functions.load()

    for name, func in six.iteritems(catalog):
        env.filters[name] = func

    return catalog.keys()


class JinjaEvaluator(base.Evaluator):
    _delimiter = '{{}}'
    _regex_pattern = '({{.*?}}|{%.*?%})'
    _regex_parser = re.compile(_regex_pattern)

    _root_ctx = {}

    _jinja_env = jinja2.Environment(
        undefined=jinja2.StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True
    )

    _custom_functions = register_functions(_jinja_env)

    @classmethod
    def validate(cls, text):
        if not isinstance(text, six.string_types):
            raise ValueError('Text to be evaluated is not typeof string.')

        errors = []

        for expr in cls._regex_parser.findall(text):
            try:
                print(cls.strip_delimiter(expr))

                parser = jinja2.parser.Parser(
                    cls._jinja_env.overlay(),
                    cls.strip_delimiter(expr),
                    state='variable'
                )

                parser.parse_expression()
            except jinja2.exceptions.TemplateError as e:
                error = {
                    'message': str(getattr(e, 'message', e)),
                    'expression': cls.strip_delimiter(expr)
                }

                errors.append(error)

        return errors

    @classmethod
    def evaluate(cls, text, data=None):
        pass
