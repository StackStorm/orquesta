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
from orchestra.expressions.functions import base as functions
from orchestra.utils import expression as utils


LOG = logging.getLogger(__name__)


def register_functions(ctx):
    catalog = functions.load()

    for name, func in six.iteritems(catalog):
        ctx.register_function(func, name=name)

    return catalog


class YaqlGrammarException(exc.ExpressionGrammarException):
    pass


class YaqlEvaluationException(exc.ExpressionEvaluationException):
    pass


class YAQLEvaluator(base.Evaluator):
    _type = 'yaql'
    _var_symbol = '$'
    _delimiter = '<%>'
    _regex_pattern = '<%.*?%>'
    _regex_parser = re.compile(_regex_pattern)
    _regex_var_pattern = '.*?(\$\.[a-zA-Z0-9_\.\[\]\(\)]*).*?'
    _regex_var_parser = re.compile(_regex_var_pattern)
    _engine = yaql.language.factory.YaqlFactory().create()
    _root_ctx = yaql.create_context()
    _custom_functions = register_functions(_root_ctx)

    @classmethod
    def contextualize(cls, data):
        ctx = cls._root_ctx.create_child_context()
        ctx['$'] = data or {}
        ctx['__task_states'] = ctx['$'].get('__task_states')

        return ctx

    @classmethod
    def has_expressions(cls, text):
        exprs = cls._regex_parser.findall(text)

        return exprs is not None and len(exprs) > 0

    @classmethod
    def validate(cls, text):
        if not isinstance(text, six.string_types):
            raise ValueError('Text to be evaluated is not typeof string.')

        errors = []

        for expr in cls._regex_parser.findall(text):
            try:
                cls._engine(cls.strip_delimiter(expr))
            except (yaql_exc.YaqlException, ValueError, TypeError) as e:
                errors.append(
                    utils.format_error(cls._type, expr, e)
                )

        return errors

    @classmethod
    def evaluate(cls, text, data=None):
        if not isinstance(text, six.string_types):
            raise ValueError('Text to be evaluated is not typeof string.')

        if data and not isinstance(data, dict):
            raise ValueError('Provided data is not typeof dict.')

        output = text
        exprs = cls._regex_parser.findall(text)
        ctx = cls.contextualize(data)

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
            raise YaqlEvaluationException(
                'Unable to resolve key \'%s\' in expression '
                '\'%s\' from context.' % (
                    str(getattr(e, 'message', e)),
                    expr
                )
            )
        except (yaql_exc.YaqlException, ValueError, TypeError) as e:
            raise YaqlEvaluationException(str(getattr(e, 'message', e)))

        return output

    @classmethod
    def extract_vars(cls, text):
        if not isinstance(text, six.string_types):
            raise ValueError('Text to be evaluated is not typeof string.')

        variables = []

        for expr in cls._regex_parser.findall(text):
            variables.extend(cls._regex_var_parser.findall(expr))

        return sorted(list(set(variables)))
