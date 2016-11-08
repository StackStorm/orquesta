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

    return catalog


class JinjaEvaluator(base.Evaluator):
    _delimiter = '{{}}'
    _regex_pattern = '({{.*?}}|{%.*?%})'
    _regex_parser = re.compile(_regex_pattern)

    _jinja_env = jinja2.Environment(
        undefined=jinja2.StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True
    )

    _custom_functions = register_functions(_jinja_env)

    @classmethod
    def contextualize(cls, data):
        ctx = {'_': data}

        for name, func in six.iteritems(cls._custom_functions):
            ctx[name] = partial(func, ctx['_'])

        if isinstance(data, dict):
            ctx['__task_states'] = data.get('__task_states')

        return ctx

    @classmethod
    def validate(cls, text):
        if not isinstance(text, six.string_types):
            raise ValueError('Text to be evaluated is not typeof string.')

        errors = []
        exprs = cls._regex_parser.findall(text)

        for expr in exprs:
            try:
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
        if not isinstance(text, six.string_types):
            raise ValueError('Text to be evaluated is not typeof string.')

        if data and not isinstance(data, dict):
            raise ValueError('Provided data is not typeof dict.')

        output = text
        exprs = cls._regex_parser.findall(text)
        ctx = cls.contextualize(data)
        opts = {'undefined_to_none': False}

        try:
            for expr in exprs:
                stripped = cls.strip_delimiter(expr)
                compiled = cls._jinja_env.compile_expression(stripped, **opts)
                result = compiled(**ctx)

                if inspect.isgenerator(result):
                    result = list(result)

                if isinstance(result, six.string_types):
                    result = cls.evaluate(result, data)

                output = (
                    output.replace(expr, str(result))
                    if len(exprs) > 1
                    else result
                )

            # For StrictUndefined values, UndefinedError only gets raised
            # when the value is accessed, not when it gets created. The
            # simplest way to access it is to try and cast it to string.
            # When StrictUndefined is cast to str below, this will raise
            # an exception with error description.
            if type(output) is jinja2.runtime.StrictUndefined:
                output = str(output)

        except jinja2.exceptions.UndefinedError as e:
            raise exc.JinjaEvaluationException(str(getattr(e, 'message', e)))

        return output
