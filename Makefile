#!/usr/bin/make

all: lint unit_test


.PHONY: clean
clean:
	@rm -rf .cache
	@rm -f .coverage
	@rm -rf .tox
	@find . -name "__pycache__" -type d -prune -exec rm -rf '{}' \;

.PHONY: apt_prereqs
apt_prereqs:
	@which charm >/dev/null || sudo snap install charm
	@# Need tox, but don't install the apt version unless we have to (don't want to conflict with pip)
	@which tox >/dev/null || (sudo apt-get install -y python-pip && sudo pip install tox)

.PHONY: lint
lint: apt_prereqs
	@tox -c tox_unit.ini --notest
	@PATH=.tox/py34/bin:.tox/py35/bin flake8 $(wildcard hooks reactive lib unit_tests tests)
	@charm proof

.PHONY: unit_test
unit_test: apt_prereqs
	@echo Starting unit tests...
	tox -c tox_unit.ini
