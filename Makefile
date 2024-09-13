# Copyright 2020-2024 StackStorm contributors.
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

PY3 := python3
SYS_PY3 := $(shell which $(PY3))
PIP_VERSION = 24.0

# Virtual Environment
VENV_DIR ?= .venv

# Tox Environment
TOX_DIR ?= .tox

# Sphinx Document Options
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SPHINXAUTO    = sphinx-autobuild
SPHINXPROJ    = Orquesta
SOURCEDIR     = docs/source
BUILDDIR      = docs/build
EGGDIR        = orquesta.egg-info

# Packaging Options
PKGDISTDIR    = dist
PKGBUILDDIR   = build


.PHONY: all
all: clean reqs schemas check package

.PHONY: clean
clean:
	rm -rf $(VENV_DIR)
	rm -rf $(TOX_DIR)
	rm -rf $(BUILDDIR)
	rm -rf $(PKGDISTDIR)
	rm -rf $(PKGBUILDDIR)
	rm -rf $(EGGDIR)

.PHONY: venv
venv:
	test -d $(VENV_DIR) || $(SYS_PY3) -m venv $(VENV_DIR)

.PHONY: reqs
reqs: venv check_virtualenv
	echo Install pip version $(PIP_VERSION) to match st2 core.
	$(VENV_DIR)/bin/pip install --upgrade "pip==$(PIP_VERSION)"
	$(VENV_DIR)/bin/pip install -r requirements.txt
	$(VENV_DIR)/bin/pip install -r requirements-test.txt
	$(VENV_DIR)/bin/pip install -r requirements-docs.txt
	$(VENV_DIR)/bin/pip install -r requirements-ci.txt
	$(VENV_DIR)/bin/python setup.py develop
	echo

.PHONY: check_virtualenv
check_virtualenv:
	test -d $(VENV_DIR) || exit 1

.PHONY: schemas
schemas: check_virtualenv
	$(VENV_DIR)/bin/$(PY3) bin/orquesta-generate-schemas

.PHONY: format
format: check_virtualenv
	$(VENV_DIR)/bin/black orquesta bin setup.py -l 100

.PHONY: check
check: check_virtualenv
	$(VENV_DIR)/bin/tox

.PHONY: docs
docs: reqs
	rm -rf $(BUILDDIR)
	. $(VENV_DIR)/bin/activate; $(SPHINXBUILD) -W -b html $(SOURCEDIR) $(BUILDDIR)/html

.PHONY: livedocs
livedocs: reqs
	rm -rf $(BUILDDIR)
	. $(VENV_DIR)/bin/activate; $(SPHINXAUTO) -H 0.0.0.0 -b html $(SOURCEDIR) $(BUILDDIR)/html

.PHONY: package
package: check_virtualenv
	rm -rf $(PKGDISTDIR)
	rm -rf $(PKGBUILDDIR)
	$(VENV_DIR)/bin/$(PY3) setup.py sdist bdist_wheel

.PHONY: publish
publish: package
	$(VENV_DIR)/bin/$(PY3) -m twine upload dist/*

