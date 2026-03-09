# This Makefile provides a set of targets for running
# a variety of development-related tasks.

IMAGE ?= hub.osg-htc.org/brian.aydemir/jupyterhub-monitoring
PY_PACKAGE_SRC := app

.PHONY: all build clean distclean docs init lint tidy update

all: tidy lint build docs

#---------------------------------------------------------------------------

init:
	poetry install

update:
	poetry update
	poetry show --outdated

tidy:
	poetry run mdformat .
	poetry run isort -q $(PY_PACKAGE_SRC)
	poetry run black -q $(PY_PACKAGE_SRC)
	poetry run typos --sort --force-exclude

lint:
	-poetry run bandit -qr $(PY_PACKAGE_SRC)
	-poetry run mypy $(PY_PACKAGE_SRC)
	-poetry run pyright $(PY_PACKAGE_SRC)
	-poetry run pylint $(PY_PACKAGE_SRC)

#---------------------------------------------------------------------------

build: clean
	poetry build

	VERSION=$$(toml get project.version --toml-path pyproject.toml); \
	docker build . \
	    --build-arg VERSION=$${VERSION} \
	    --pull \
	    -t $(IMAGE):$${VERSION}

docs:
	poetry run sphinx-apidoc --separate --no-toc -o docs/api $(PY_PACKAGE_SRC)
	rm -f docs/api/app.rst
	poetry run sphinx-build -b html docs docs/_build/html

clean:
	rm -rf dist docs/_build docs/api

distclean: clean
	git clean -x -d --force --exclude=.python-version
