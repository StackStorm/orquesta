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

import inspect
import logging
import re
import six

import yaql
import yaql.language.exceptions as yaql_exc

from orchestra import exceptions as exc
from orchestra.expressions import base
from orchestra.expressions import yaql_functions


LOG = logging.getLogger(__name__)


class YAQLEvaluator(base.Evaluator):
    _delimiter = '<%>'
    _regex_pattern = '<%.*?%>'
    _regex_parser = re.compile(_regex_pattern)
    _engine = yaql.language.factory.YaqlFactory().create()
    _root_ctx = yaql.create_context()
    _custom_functions = yaql_functions.register_functions(_root_ctx)

    @classmethod
    def strip_delimiter(cls, expr):
        return expr.strip(cls._delimiter).strip()

    @classmethod
    def validate(cls, text):
        if not isinstance(text, six.string_types):
            raise ValueError('Text to be evaluated is not typeof string.')

        errors = []

        for expr in cls._regex_parser.findall(text):
            try:
                cls._engine(cls.strip_delimiter(expr))
            except (yaql_exc.YaqlException, ValueError, TypeError) as e:
                error = {
                    'message': str(getattr(e, 'message', e)),
                    'expression': cls.strip_delimiter(expr)
                }

                errors.append(error)

        return errors

    @classmethod
    def evaluate(cls, text, data=None):
        if not isinstance(text, six.string_types):
            raise ValueError('Text to be evaluated is not typeof string.')

        if data and not isinstance(data, dict):
            raise ValueError('Provided data is not typeof dict.')

        output = text
        exprs = cls._regex_parser.findall(text)
        ctx = cls._root_ctx.create_child_context()
        ctx['$'] = data or {}
        ctx['__tasks'] = ctx['$'].get('__tasks')

        try:
            for expr in exprs:
                stripped = cls.strip_delimiter(expr)
                result = cls._engine(stripped).evaluate(context=ctx)

                if inspect.isgenerator(result):
                    result = list(result)

                if isinstance(result, six.string_types):
                    result = cls.evaluate(result, data)

                output = (
                    output.replace(expr, str(result))
                    if len(exprs) > 1
                    else result
                )

        except KeyError as e:
            raise exc.YaqlEvaluationException(
                'Unable to resolve key \'%s\' in expression '
                '\'%s\' from context.' % (
                    str(getattr(e, 'message', e)),
                    expr
                )
            )
        except (yaql_exc.YaqlException, ValueError, TypeError) as e:
            raise exc.YaqlEvaluationException(str(getattr(e, 'message', e)))

        return output
