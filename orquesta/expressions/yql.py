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

from orquesta import exceptions as exc
from orquesta.expressions import base
from orquesta.expressions.functions import base as functions
from orquesta.utils import expression as utils
from orquesta.utils import strings as strings_utils


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
    _delimiter = '<%>'
    _regex_pattern = '<%.*?%>'
    _regex_parser = re.compile(_regex_pattern)

    _regex_dot_pattern = '[a-zA-Z0-9_\'"\.\[\]\(\)]*'
    _regex_ctx_pattern_1 = 'ctx\(\)\.%s' % _regex_dot_pattern
    _regex_ctx_pattern_2 = 'ctx\([\'|"]?{0}[\'|"]?\)[\.{0}]?'.format(_regex_dot_pattern)
    _regex_var_pattern = '.*?(%s|%s).*?' % (_regex_ctx_pattern_1, _regex_ctx_pattern_2)
    _regex_var_parser = re.compile(_regex_var_pattern)

    _regex_dot_extract = '([a-zA-Z0-9_\-]*)'
    _regex_ctx_extract_1 = 'ctx\(\)\.%s' % _regex_dot_extract
    _regex_ctx_extract_2 = 'ctx\([\'|"]?%s(%s)' % (_regex_dot_extract, _regex_dot_pattern)
    _regex_var_extracts = ['%s\.?' % _regex_ctx_extract_1, '%s\.?' % _regex_ctx_extract_2]

    _engine = yaql.language.factory.YaqlFactory().create()
    _root_ctx = yaql.create_context()
    _custom_functions = register_functions(_root_ctx)

    @classmethod
    def contextualize(cls, data):
        ctx = cls._root_ctx.create_child_context()
        ctx['__vars'] = data or {}
        ctx['__flow'] = ctx['__vars'].get('__flow')
        ctx['__current_task'] = ctx['__vars'].get('__current_task')
        ctx['__current_item'] = ctx['__vars'].get('__current_item')

        return ctx

    @classmethod
    def get_statement_regex(cls):
        return cls._regex_pattern

    @classmethod
    def has_expressions(cls, text):
        exprs = cls._regex_parser.findall(text)

        return exprs is not None and len(exprs) > 0

    @classmethod
    def get_var_extraction_regexes(cls):
        return cls._regex_var_extracts

    @classmethod
    def validate(cls, text):
        if not isinstance(text, six.string_types):
            raise ValueError('Text to be evaluated is not typeof string.')

        errors = []

        for expr in cls._regex_parser.findall(text):
            try:
                cls._engine(cls.strip_delimiter(expr))
            except (yaql_exc.YaqlException, ValueError, TypeError) as e:
                errors.append(utils.format_error(cls._type, expr, e))

        return errors

    @classmethod
    def evaluate(cls, text, data=None):
        if not isinstance(text, six.string_types):
            raise ValueError('Text to be evaluated is not typeof string.')

        if data and not isinstance(data, dict):
            raise ValueError('Provided data is not typeof dict.')

        output = strings_utils.unicode(text)
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

                if len(exprs) > 1 or len(output) > len(expr):
                    output = output.replace(expr, strings_utils.unicode(result, force=True))
                else:
                    output = strings_utils.unicode(result)

        except KeyError as e:
            error = str(getattr(e, 'message', e)).strip("'")
            msg = "Unable to resolve key '%s' in expression '%s' from context."
            raise YaqlEvaluationException(msg % (error, expr))
        except (yaql_exc.YaqlException, ValueError, TypeError) as e:
            msg = "Unable to evaluate expression '%s'. %s: %s"
            raise YaqlEvaluationException(msg % (expr, e.__class__.__name__, str(e).strip("'")))
        except Exception as e:
            msg = "Unable to evaluate expression '%s'. %s: %s"
            raise YaqlEvaluationException(msg % (expr, e.__class__.__name__, str(e)))

        return output

    @classmethod
    def extract_vars(cls, text):
        if not isinstance(text, six.string_types):
            raise ValueError('Text to be evaluated is not typeof string.')

        variables = []

        for expr in cls._regex_parser.findall(text):
            variables.extend(cls._regex_var_parser.findall(expr))

        return sorted(list(set(variables)))
