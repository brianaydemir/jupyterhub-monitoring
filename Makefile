# This Makefile provides a set of targets for performing a variety of
# development-related tasks.

IMAGE ?= hub.osg-htc.org/brian.aydemir/jupyterhub-monitoring
PY_PACKAGE_SRC := app

.PHONY: all build clean distclean docs init lint reformat update

all: reformat lint build

#---------------------------------------------------------------------------

init:
	poetry install

reformat:
	poetry run mdformat .
	poetry run isort -q $(PY_PACKAGE_SRC)
	poetry run black -q $(PY_PACKAGE_SRC)

lint:
	-poetry run bandit -qr $(PY_PACKAGE_SRC)
	-poetry run mypy $(PY_PACKAGE_SRC)
	-poetry run pylint $(PY_PACKAGE_SRC)

update:
	poetry update
	poetry show --outdated
	make requirements.txt

requirements.txt: poetry.lock
	poetry export > requirements.txt

#---------------------------------------------------------------------------

build: clean
	poetry build

	VERSION=$$(toml get project.version --toml-path pyproject.toml); \
	docker build . \
	    --build-arg VERSION=$${VERSION} \
	    --pull \
	    -t $(IMAGE):$${VERSION}

docs:
	poetry run sphinx-apidoc -o docs/api $(PY_PACKAGE_SRC)
	poetry run sphinx-build -b html docs docs/_build/html

clean:
	rm -rf dist docs/_build docs/api

distclean:
	git clean -x -d --force --exclude=.python-version
