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

import abc
import inspect
import logging
import re
import six
import threading

from stevedore import extension

from orquesta.utils import expression as expr_util
from orquesta.utils import plugin as plugin_util


LOG = logging.getLogger(__name__)

_EXP_EVALUATORS = None
_EXP_EVALUATORS_LOCK = threading.Lock()
_EXP_EVALUATOR_NAMESPACE = "orquesta.expressions.evaluators"


@six.add_metaclass(abc.ABCMeta)
class Evaluator(object):
    _type = "unspecified"
    _delimiter = None

    @classmethod
    def get_type(cls):
        return cls._type

    @classmethod
    def strip_delimiter(cls, expr):
        return expr.strip(cls._delimiter).strip()

    @classmethod
    def get_statement_regex(cls):
        raise NotImplementedError()

    @classmethod
    def has_expressions(cls, text):
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def validate(cls, statement):
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def evaluate(cls, text, data=None):
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def extract_vars(cls, statement):
        raise NotImplementedError()


def get_evaluator(language):
    return plugin_util.get_module(_EXP_EVALUATOR_NAMESPACE, language)


def get_evaluators():
    global _EXP_EVALUATORS
    global _EXP_EVALUATORS_LOCK

    with _EXP_EVALUATORS_LOCK:
        if _EXP_EVALUATORS is None:
            _EXP_EVALUATORS = {}

            mgr = extension.ExtensionManager(
                namespace=_EXP_EVALUATOR_NAMESPACE, invoke_on_load=False
            )

            for name in mgr.names():
                _EXP_EVALUATORS[name] = get_evaluator(name)

    return _EXP_EVALUATORS


def get_statement_regexes():
    return {t: e.get_statement_regex() for t, e in six.iteritems(get_evaluators())}


def has_expressions(text):
    result = {t: e.has_expressions(text) for t, e in six.iteritems(get_evaluators())}

    return any(result.values())


def validate(statement):
    errors = []

    if isinstance(statement, dict):
        for k, v in six.iteritems(statement):
            errors.extend(validate(k)["errors"])
            errors.extend(validate(v)["errors"])

    elif isinstance(statement, list):
        for item in statement:
            errors.extend(validate(item)["errors"])

    elif isinstance(statement, six.string_types):
        evaluators = [
            evaluator
            for name, evaluator in six.iteritems(get_evaluators())
            if evaluator.has_expressions(statement)
        ]

        if len(evaluators) == 1:
            errors.extend(evaluators[0].validate(statement))
        elif len(evaluators) > 1:
            message = "Expression with multiple types is not supported."
            errors.append(expr_util.format_error(None, statement, message))

    return {"errors": errors}


def evaluate(statement, data=None):
    if isinstance(statement, dict):
        return {evaluate(k, data=data): evaluate(v, data=data) for k, v in six.iteritems(statement)}

    elif isinstance(statement, list):
        return [evaluate(item, data=data) for item in statement]

    elif isinstance(statement, six.string_types):
        for name, evaluator in six.iteritems(get_evaluators()):
            if evaluator.has_expressions(statement):
                return evaluator.evaluate(statement, data=data)

    return statement


def extract_vars(statement):
    variables = []

    if isinstance(statement, dict):
        for k, v in six.iteritems(statement):
            variables.extend(extract_vars(k))
            variables.extend(extract_vars(v))

    elif isinstance(statement, list):
        for item in statement:
            variables.extend(extract_vars(item))

    elif isinstance(statement, six.string_types):
        for name, evaluator in six.iteritems(get_evaluators()):
            for var_ref in evaluator.extract_vars(statement):
                for regex_var_extract in evaluator.get_var_extraction_regexes():
                    result = re.search(regex_var_extract, var_ref)
                    var = result.group(1) if result else ""
                    variables.append((evaluator.get_type(), statement, var))

    variables = [v for v in variables if v[2] != ""]

    return sorted(list(set(variables)), key=lambda var: var[2])


def func_has_ctx_arg(func):
    getargspec = (
        inspect.getargspec if six.PY2 else inspect.getfullargspec  # pylint: disable=no-member
    )

    return "context" in getargspec(func).args
