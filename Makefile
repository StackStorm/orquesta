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

SHELL := /bin/bash
PY27 := /usr/bin/python2.7
RPM_ROOT=~/rpmbuild
RPM_SOURCES_DIR := $(RPM_ROOT)/SOURCES/
RPM_SPECS_DIR := $(RPM_ROOT)/SPECS/
VER := $(shell cat ./orchestra/__init__.py | grep -Po "__version__ = '\K[^']*")
RELEASE=1
COMPONENTS := orchestra

.PHONY: rpm
rpm: 
	$(PY27) setup.py bdist_rpm --python=$(PY27)
	mkdir -p $(RPM_ROOT)/RPMS/noarch
	cp dist/$(COMPONENTS)*noarch.rpm $(RPM_ROOT)/RPMS/noarch/$(COMPONENTS)-$(VER)-$(RELEASE).noarch.rpm
	mkdir -p $(RPM_ROOT)/SRPMS
	cp dist/*src.rpm $(RPM_ROOT)/SRPMS/$(COMPONENTS)-$(VER)-$(RELEASE).src.rpm
	rm -Rf dist $(COMPONENTS).egg-info ChangeLog AUTHORS build

.PHONY: rhel-rpm
rhel-rpm:
	$(PY27) setup.py bdist_rpm --python=$(PY27)
	mkdir -p $(RPM_ROOT)/RPMS/noarch
	cp dist/$(COMPONENTS)*noarch.rpm $(RPM_ROOT)/RPMS/noarch/$(COMPONENTS)-$(VER)-$(RELEASE).noarch.rpm
	mkdir -p $(RPM_ROOT)/SRPMS
	cp dist/*src.rpm $(RPM_ROOT)/SRPMS/$(COMPONENTS)-$(VER)-$(RELEASE).src.rpm
	rm -Rf dist $(COMPONENTS).egg-info ChangeLog AUTHORS build

.PHONY: deb
deb:
	$(PY27) setup.py --command-packages=stdeb.command bdist_deb
	mkdir -p ~/debbuild
	cp deb_dist/python-$(COMPONENTS)*-1_all.deb ~/debbuild/$(COMPONENTS)_$(VER)-$(RELEASE)_amd64.deb
	rm -Rf dist deb_dist $(COMPONENTS)-$(VER).tar.gz $(COMPONENTS).egg-info ChangeLog AUTHORS build
