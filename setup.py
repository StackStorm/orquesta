#!/usr/bin/env python2.7
#
# Copyright 2021 The StackStorm Authors.
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

import os
import sys

from setuptools import setup, find_packages


PKG_ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
PKG_REQ_FILE = "%s/requirements.txt" % PKG_ROOT_DIR
os.chdir(PKG_ROOT_DIR)


def get_version_string():
    version = None
    sys.path.insert(0, PKG_ROOT_DIR)
    from orquesta import __version__

    version = __version__
    sys.path.pop(0)
    return version


def get_requirements():
    with open(PKG_REQ_FILE) as f:
        required = f.read().splitlines()

    # Ignore comments in the requirements file
    required = [line for line in required if not line.startswith("#")]
    return required


setup(
    name="orquesta",
    version=get_version_string(),
    author="StackStorm",
    author_email="info@stackstorm.com",
    url="https://www.stackstorm.com",
    packages=find_packages(exclude=[]),
    install_requires=get_requirements(),
    license="Apache License (2.0)",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
    ],
    entry_points={
        "orquesta.composers": [
            "native = orquesta.composers.native:WorkflowComposer",
            "mistral = orquesta.composers.mistral:WorkflowComposer",
            "mock = orquesta.composers.mock:WorkflowComposer",
        ],
        "orquesta.expressions.evaluators": [
            "yaql = orquesta.expressions.yql:YAQLEvaluator",
            "jinja = orquesta.expressions.jinja:JinjaEvaluator",
        ],
        "orquesta.expressions.functions": [
            "ctx = orquesta.expressions.functions.common:ctx_",
            "json = orquesta.expressions.functions.common:json_",
            "zip = orquesta.expressions.functions.common:zip_",
            "item = orquesta.expressions.functions.workflow:item_",
            "task_status = orquesta.expressions.functions.workflow:task_status_",
            "succeeded = orquesta.expressions.functions.workflow:succeeded_",
            "failed = orquesta.expressions.functions.workflow:failed_",
            "completed = orquesta.expressions.functions.workflow:completed_",
            "result = orquesta.expressions.functions.workflow:result_",
        ],
        "orquesta.tests": [
            "fake = orquesta.tests.unit.utils.test_plugin:FakePlugin",
        ],
        # TODO: Find alternative way to run these checks. Adding extensions here
        # affect downstream applications.
        # "flake8.extension": [
        #     "O101 = orquesta.tests.hacking.import_modules_rule:check_module_only",
        #     "O102 = orquesta.tests.hacking.import_aliases_rule:check_alias_naming",
        # ],
    },
    scripts=[
        "bin/orquesta-generate-schemas",
        "bin/orquesta-rehearse",
    ],
)
