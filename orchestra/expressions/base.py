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
import logging
import re
import six

from stevedore import extension

from orchestra.expressions import utils
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
    def get_type(cls):
        return cls._type

    @classmethod
    def strip_delimiter(cls, expr):
        return expr.strip(cls._delimiter).strip()

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


def validate(value):

    errors = []

    if isinstance(value, dict):
        for k, v in six.iteritems(value):
            errors.extend(validate(k)['errors'])
            errors.extend(validate(v)['errors'])

    elif isinstance(value, list):
        for item in value:
            errors.extend(validate(item)['errors'])

    elif isinstance(value, six.string_types):
        evaluators = [
            evaluator for name, evaluator in six.iteritems(get_evaluators())
            if evaluator.has_expressions(value)
        ]

        if len(evaluators) == 1:
            errors.extend(evaluators[0].validate(value))
        elif len(evaluators) > 1:
            message = 'Expression with multiple types is not supported.'
            errors.append(utils.format_error(None, value, message))

    return {'errors': errors}


def evaluate(text, data=None):
    for name, evaluator in six.iteritems(get_evaluators()):
        if evaluator.has_expressions(text):
            return evaluator.evaluate(text, data=data)

    return text


def extract_vars(text):
    variables = []

    for name, evaluator in six.iteritems(get_evaluators()):
        regex_var_extract = _REGEX_VAR_EXTRACT % evaluator._var_symbol

        for var_ref in evaluator.extract_vars(text):
            var = {
                'type': evaluator.get_type(),
                'expression': text,
                'name': re.search(regex_var_extract, var_ref).group(1)
            }

            if not list(filter(lambda x: x['name'] == var['name'], variables)):
                variables.append(var)

    return sorted(variables, key=lambda var: var['name'])
