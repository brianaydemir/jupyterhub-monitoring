# This Makefile provides targets for development-related tasks.

IMAGE ?= hub.osg-htc.org/brian.aydemir/jupyterhub-monitoring
PY_PACKAGE_SRC := app

.PHONY: all build clean distclean init lint tidy update

all: tidy lint build

#---------------------------------------------------------------------------

init:
	poetry install
	poetry run pre-commit install

update:
	poetry update
	poetry show --outdated

tidy:
	poetry run isort -q $(PY_PACKAGE_SRC)
	poetry run black -q $(PY_PACKAGE_SRC)
	poetry run mdformat .
	poetry run typos --sort .

lint:
	-poetry run bandit -qr $(PY_PACKAGE_SRC)
	-poetry run mypy $(PY_PACKAGE_SRC)
	-poetry run pyright $(PY_PACKAGE_SRC)
	-poetry run pylint $(PY_PACKAGE_SRC)

#---------------------------------------------------------------------------

build:
	poetry build

	VERSION=$$(toml get project.version --toml-path pyproject.toml); \
	docker build . \
	    --build-arg VERSION=$${VERSION} \
	    --pull \
	    -t $(IMAGE):$${VERSION}

clean:
	rm -rf dist

distclean:
	git clean -x -d --force --exclude=.python-version
