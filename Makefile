# This Makefile provides a set of targets for performing a variety of
# development-related tasks.

.PHONY: all build clean init lint reformat

PY_PACKAGE_SRC := app

all: reformat lint build

#---------------------------------------------------------------------------

init:
	poetry sync
	poetry show --outdated
	make requirements.txt

reformat:
	poetry run isort -q $(PY_PACKAGE_SRC)
	poetry run black -q $(PY_PACKAGE_SRC)

lint:
	-poetry run bandit -qr $(PY_PACKAGE_SRC)
	-poetry run mypy $(PY_PACKAGE_SRC)
	-poetry run pylint $(PY_PACKAGE_SRC)

requirements.txt: poetry.lock
	poetry export > requirements.txt

#---------------------------------------------------------------------------

build: clean
	poetry build

	version=$$(toml get tool.poetry.version --toml-path pyproject.toml); \
	docker build . \
	    --build-arg VERSION=$${version} \
	    --pull \
	    -t hub.osg-htc.org/brian.aydemir/jupyterhub-monitoring:$${version}

clean:
	rm -rf dist .mypy_cache
