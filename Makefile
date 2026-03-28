# This Makefile provides targets for development-related tasks.

IMAGE ?= hub.osg-htc.org/brian.aydemir/jupyterhub-monitoring
PY_PACKAGE_SRC := app
PY_TEST_SRC := tests

# Each linter below is prefixed with this, so that a local `make lint` runs
# all of them and reports every problem at once. Override it with an empty
# value (`make lint IGNORE_ERRORS=`) to stop at the first failure, which is
# what CI wants.
IGNORE_ERRORS ?= -

.PHONY: all build clean distclean init lint test tidy update

all: tidy lint test build

#---------------------------------------------------------------------------

init:
	poetry install
	poetry run pre-commit install

update:
	poetry update
	poetry show --outdated

tidy:
	poetry run isort -q $(PY_PACKAGE_SRC) $(PY_TEST_SRC)
	poetry run black -q $(PY_PACKAGE_SRC) $(PY_TEST_SRC)
	poetry run mdformat .
	poetry run typos --sort .

lint:
	$(IGNORE_ERRORS)poetry run bandit -c pyproject.toml -qr $(PY_PACKAGE_SRC) $(PY_TEST_SRC)
	$(IGNORE_ERRORS)poetry run mypy $(PY_PACKAGE_SRC) $(PY_TEST_SRC)
	$(IGNORE_ERRORS)poetry run pyright $(PY_PACKAGE_SRC) $(PY_TEST_SRC)
	$(IGNORE_ERRORS)poetry run pylint $(PY_PACKAGE_SRC) $(PY_TEST_SRC)

test:
	poetry run pytest

#---------------------------------------------------------------------------

# Depends on clean so that a stale wheel left in dist/ cannot make the
# Dockerfile's version glob match more than one file.
build: clean
	poetry build

	VERSION=$$(poetry run toml get project.version --toml-path pyproject.toml); \
	docker build . \
	    --build-arg VERSION=$${VERSION} \
	    --pull \
	    -t $(IMAGE):$${VERSION}

clean:
	rm -rf dist

distclean:
	git clean -x -d --force --exclude=.python-version
