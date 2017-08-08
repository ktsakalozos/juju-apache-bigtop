#!/usr/bin/make

all: lint unit_test


.PHONY: clean
clean:
	@rm -rf .cache
	@rm -f .coverage
	@rm -rf .tox
	@find . -name "__pycache__" -type d -prune -exec rm -rf '{}' \;

.PHONY: sysdeps
sysdeps:
	@which charm >/dev/null || (sudo apt-get install -y snapd && sudo snap install charm --classic)
	@which tox >/dev/null || (sudo apt-get install -y python-pip && sudo pip install tox)

.PHONY: lint
lint: sysdeps
	@echo Starting linter...
	@tox -c tox_unit.ini --notest
	@PATH=.tox/py34/bin:.tox/py35/bin flake8 $(wildcard hooks reactive lib unit_tests tests)
	@echo Starting proof...
	@charm proof

.PHONY: unit_test
unit_test: sysdeps
	@echo Starting unit tests...
	@tox -c tox_unit.ini
