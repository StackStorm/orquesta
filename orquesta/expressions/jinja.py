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

import functools
import inspect
import itertools
import logging
import re
import six

import jinja2

from orquesta import exceptions as exc
from orquesta.expressions import base as expr_base
from orquesta.expressions.functions import base as func_base
from orquesta.utils import expression as expr_util
from orquesta.utils import strings as str_util


LOG = logging.getLogger(__name__)


def register_functions(env):
    catalog = func_base.load()

    for name, func in six.iteritems(catalog):
        env.filters[name] = func

    return catalog


class JinjaGrammarException(exc.ExpressionGrammarException):
    pass


class JinjaEvaluationException(exc.ExpressionEvaluationException):
    pass


class JinjaEvaluator(expr_base.Evaluator):
    _type = "jinja"
    _delimiter = "{{}}"
    _regex_pattern = "{{.*?}}"
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

    _block_delimiter = "{%}"
    _regex_block_pattern = "{%.*?%}"
    _regex_block_parser = re.compile(_regex_block_pattern)

    _regex_raw_block_pattern = "{% raw %}.*?{% endraw %}"
    _regex_raw_block_parser = re.compile(_regex_raw_block_pattern)

    _jinja_env = jinja2.Environment(
        undefined=jinja2.StrictUndefined, trim_blocks=True, lstrip_blocks=True
    )

    _custom_functions = register_functions(_jinja_env)

    @classmethod
    def contextualize(cls, data):
        ctx = {"__vars": data}

        if isinstance(data, dict):
            ctx["__state"] = ctx["__vars"].get("__state")
            ctx["__current_task"] = ctx["__vars"].get("__current_task")
            ctx["__current_item"] = ctx["__vars"].get("__current_item")

        for name, func in six.iteritems(cls._custom_functions):
            ctx[name] = functools.partial(func, ctx) if expr_base.func_has_ctx_arg(func) else func

        return ctx

    @classmethod
    def get_statement_regex(cls):
        return cls._regex_pattern

    @classmethod
    def has_expressions(cls, text):
        exprs = cls._regex_parser.findall(text)
        block_exprs = cls._regex_block_parser.findall(text)

        return exprs or block_exprs

    @classmethod
    def get_var_extraction_regexes(cls):
        return cls._regex_var_extracts

    @classmethod
    def validate(cls, text):
        if not isinstance(text, six.string_types):
            raise ValueError("Text to be evaluated is not typeof string.")

        errors = []

        # Validate the entire text to cover malformed delimiters and blocks.
        try:
            cls._jinja_env.parse(text)
        except jinja2.exceptions.TemplateError as e:
            errors.append(expr_util.format_error(cls._type, text, e))

        # Validate individual inline expressions.
        for expr in cls._regex_parser.findall(text):
            # Skip expression if it has already been validated and erred.
            if list(filter(lambda x: x["expression"] == expr, errors)):
                continue

            try:
                parser = jinja2.parser.Parser(
                    cls._jinja_env.overlay(), cls.strip_delimiter(expr), state="variable"
                )

                parser.parse_expression()
            except jinja2.exceptions.TemplateError as e:
                errors.append(expr_util.format_error(cls._type, expr, e))

        return errors

    @classmethod
    def _evaluate_and_expand(cls, text, data=None):
        exprs = cls._regex_parser.findall(text)
        block_exprs = cls._regex_block_parser.findall(text)
        ctx = cls.contextualize(data)
        opts = {"undefined_to_none": False}

        try:
            # If there is a Jinja block expression in the text, then process the whole text.
            if block_exprs:
                expr = text
                output = cls._jinja_env.from_string(expr).render(ctx)
                output = str_util.unicode(output)

                # Traverse and evaulate again in case additional inline epxressions are
                # introduced after the jinja block is evaluated.
                output = cls._evaluate_and_expand(output, data)
            else:
                # The output will first be the original text and the expressions
                # will be substituted by the evaluated value.
                output = str_util.unicode(text)

                # Evaluate inline jinja expressions first.
                for expr in exprs:
                    stripped = cls.strip_delimiter(expr)
                    compiled = cls._jinja_env.compile_expression(stripped, **opts)
                    result = compiled(**ctx)

                    if inspect.isgenerator(result):
                        result = list(result)

                    if isinstance(result, six.string_types):
                        result = cls._evaluate_and_expand(result, data)

                    # For StrictUndefined values, UndefinedError only gets raised when the value is
                    # accessed, not when it gets created. The simplest way to access it is to try
                    # and cast it to string. When StrictUndefined is cast to str below, this will
                    # raise an exception with error description.
                    if not isinstance(result, jinja2.runtime.StrictUndefined):
                        if len(exprs) > 1 or block_exprs or len(output) > len(expr):
                            output = output.replace(expr, str_util.unicode(result, force=True))
                        else:
                            output = str_util.unicode(result)

        except jinja2.exceptions.UndefinedError as e:
            msg = "Unable to evaluate expression '%s'. %s: %s"
            raise JinjaEvaluationException(msg % (expr, e.__class__.__name__, str(e)))
        except Exception as e:
            msg = "Unable to evaluate expression '%s'. %s: %s"
            raise JinjaEvaluationException(msg % (expr, e.__class__.__name__, str(e)))

        return output

    @classmethod
    def evaluate(cls, text, data=None):
        if not isinstance(text, six.string_types):
            raise ValueError("Text to be evaluated is not typeof string.")

        if data and not isinstance(data, dict):
            raise ValueError("Provided data is not typeof dict.")

        # Remove raw blocks from the expression.
        raw_blocks = cls._regex_raw_block_parser.findall(text)

        for i in range(0, len(raw_blocks)):
            text = text.replace(raw_blocks[i], "{%s}" % str(i))

        # Recursively evaluate the expression.
        output = cls._evaluate_and_expand(text, data=data)

        if isinstance(output, six.string_types):
            exprs = [cls.strip_delimiter(expr) for expr in cls._regex_parser.findall(output)]

            if exprs:
                raise JinjaEvaluationException(
                    "There are unresolved variables: %s" % ", ".join(exprs)
                )

        if isinstance(output, six.string_types) and raw_blocks:
            # Put raw blocks back into the expression.
            for i in range(0, len(raw_blocks)):
                output = output.replace("{%s}" % str(i), raw_blocks[i])  # pylint: disable=E1101

            # Evaluate the raw blocks.
            ctx = cls.contextualize(data)
            output = cls._jinja_env.from_string(output).render(ctx)

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
