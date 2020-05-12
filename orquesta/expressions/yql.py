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

import inspect
import itertools
import logging
import re
import six

import yaql
import yaql.language.exceptions as yaql_exc
import yaql.language.utils as yaql_utils

from orquesta import exceptions as exc
from orquesta.expressions import base as expr_base
from orquesta.expressions.functions import base as func_base
from orquesta.utils import expression as expr_util
from orquesta.utils import strings as str_util


LOG = logging.getLogger(__name__)


def register_functions(ctx):
    catalog = func_base.load()

    for name, func in six.iteritems(catalog):
        ctx.register_function(func, name=name)

    return catalog


class YaqlGrammarException(exc.ExpressionGrammarException):
    pass


class YaqlEvaluationException(exc.ExpressionEvaluationException):
    pass


class YAQLEvaluator(expr_base.Evaluator):
    _type = "yaql"
    _delimiter = "<%>"
    _regex_pattern = "<%.*?%>"
    _regex_parser = re.compile(_regex_pattern)

    _regex_ctx_ref_pattern = r'[][a-zA-Z0-9_\'"\.()]*'
    # match any of:
    #   word boundary ctx(*)
    #   word boundary ctx()*
    #   word boundary ctx().*
    #   word boundary ctx(*)*
    #   word boundary ctx(*).*
    _regex_ctx_pattern = r'\bctx\([\'"]?{0}[\'"]?\)\.?{0}'.format(_regex_ctx_ref_pattern)
    _regex_ctx_var_parser = re.compile(_regex_ctx_pattern)

    _regex_var = r"[a-zA-Z0-9_-]+"
    _regex_var_extracts = [
        r"(?<=\bctx\(\)\.)({})\b(?!\()\.?".format(_regex_var),  # extract x in ctx().x
        r"(?:\bctx\(({})\))".format(_regex_var),  # extract x in ctx(x)
        r"(?:\bctx\(\'({})\'\))".format(_regex_var),  # extract x in ctx('x')
        r'(?:\bctx\("({})"\))'.format(_regex_var),  # extract x in ctx("x")
    ]

    _engine = yaql.language.factory.YaqlFactory().create()
    _root_ctx = yaql.create_context()
    _custom_functions = register_functions(_root_ctx)

    @classmethod
    def contextualize(cls, data):
        ctx = cls._root_ctx.create_child_context()

        # Some yaql expressions (e.g. distinct()) refer to hash value of variable.
        # But some built-in Python type values (e.g. list and dict) don't have __hash__() method.
        # The convert_input_data method parses specified variable and convert it to hashable one.
        if isinstance(data, yaql_utils.SequenceType) or isinstance(data, yaql_utils.MappingType):
            ctx["__vars"] = yaql_utils.convert_input_data(data)
        else:
            ctx["__vars"] = data or {}

        ctx["__state"] = ctx["__vars"].get("__state")
        ctx["__current_task"] = ctx["__vars"].get("__current_task")
        ctx["__current_item"] = ctx["__vars"].get("__current_item")

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
            raise ValueError("Text to be evaluated is not typeof string.")

        errors = []

        for expr in cls._regex_parser.findall(text):
            try:
                cls._engine(cls.strip_delimiter(expr))
            except (yaql_exc.YaqlException, ValueError, TypeError) as e:
                errors.append(expr_util.format_error(cls._type, expr, e))

        return errors

    @classmethod
    def evaluate(cls, text, data=None):
        if not isinstance(text, six.string_types):
            raise ValueError("Text to be evaluated is not typeof string.")

        if data and not isinstance(data, dict):
            raise ValueError("Provided data is not typeof dict.")

        output = str_util.unicode(text)
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
                    output = output.replace(expr, str_util.unicode(result, force=True))
                else:
                    output = str_util.unicode(result)

        except KeyError as e:
            error = str(getattr(e, "message", e)).strip("'")
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
            raise ValueError("Text to be evaluated is not typeof string.")

        results = [
            cls._regex_ctx_var_parser.findall(expr.strip(cls._delimiter).strip())
            for expr in cls._regex_parser.findall(text)
        ]

        variables = [v.strip() for v in itertools.chain.from_iterable(results)]

        return sorted(list(set(variables)))
