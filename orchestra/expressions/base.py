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

import abc
import json
import logging
import re
import six

from stevedore import extension

from orchestra import exceptions as exc
from orchestra.utils import plugin


LOG = logging.getLogger(__name__)

_EXP_EVALUATORS = None
_EXP_EVALUATOR_NAMESPACE = 'orchestra.expressions.evaluators'
_REGEX_VAR_EXTRACT = '\%s\.([a-zA-Z0-9_\-]*)\.?'


@six.add_metaclass(abc.ABCMeta)
class Evaluator(object):
    _type = 'unspecified'
    _delimiter = None

    @classmethod
    def strip_delimiter(cls, expr):
        return expr.strip(cls._delimiter).strip()

    @classmethod
    def format_error(cls, expr, exc):
        return {
            'type': cls._type,
            'expression': expr,
            'message': str(getattr(exc, 'message', exc))
        }

    @classmethod
    def has_expressions(cls, text):
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def validate(cls, text):
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def evaluate(cls, text, data=None):
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def extract_vars(cls, text):
        raise NotImplementedError()


def get_evaluator(language):
    return plugin.get_module(_EXP_EVALUATOR_NAMESPACE, language)


def get_evaluators():
    global _EXP_EVALUATORS

    if _EXP_EVALUATORS is None:
        _EXP_EVALUATORS = {}

        mgr = extension.ExtensionManager(
            namespace=_EXP_EVALUATOR_NAMESPACE,
            invoke_on_load=False
        )

        for name in mgr.names():
            _EXP_EVALUATORS[name] = get_evaluator(name)

    return _EXP_EVALUATORS


def validate(text):
    result = [
        {
            'type': name,
            'module': evaluator,
            'errors': evaluator.validate(text)
        }
        for name, evaluator in six.iteritems(get_evaluators())
        if evaluator.has_expressions(text)
    ]

    if len(result) == 1:
        return result[0]
    else:
        error = Evaluator.format_error(
            text,
            exc.ExpressionGrammarException(
                'The statement contains multiple expression '
                'types which is not supported.'
            )
        )

        return {
            'type': None,
            'module': None,
            'errors': [error]
        }


def evaluate(text, data=None):
    result = validate(text)
    errors = result.get('errors', [])

    if len(errors) > 0:
        raise exc.ExpressionGrammarException(
            'Validation failed for expression. %s', json.dumps(errors)
        )

    evaluator = result.get('module')

    return evaluator.evaluate(text, data=data)


def extract_vars(text):
    variables = []

    for name, evaluator in six.iteritems(get_evaluators()):
        regex_var_extract = _REGEX_VAR_EXTRACT % evaluator._var_symbol

        for var_ref in evaluator.extract_vars(text):
            variables.append(re.search(regex_var_extract, var_ref).group(1))

    return sorted(list(set(variables)))
