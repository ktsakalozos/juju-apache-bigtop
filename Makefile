#!/usr/bin/make

all: lint unit_test


.PHONY: clean
clean:
	@rm -rf .cache
	@rm -f .coverage
	@rm -rf .tox
	@find . -name "*.pyc" -type f -exec rm -f '{}' \;
	@find . -name "__pycache__" -type d -prune -exec rm -rf '{}' \;

.PHONY: sysdeps
sysdeps:
	@which tox >/dev/null || (sudo apt-get install -y python-pip && sudo pip install tox)

.PHONY: lint
lint: sysdeps
	@echo Starting linter...
	@tox -c tox_unit.ini -e lint

.PHONY: unit_test
unit_test: sysdeps
	@echo Starting unit tests...
	@tox -c tox_unit.ini
