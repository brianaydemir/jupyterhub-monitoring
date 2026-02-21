# This Makefile provides a set of targets for performing a variety of
# development-related tasks.

PY_PACKAGE_SRC := app

.PHONY: all build clean init lint reformat update

all: reformat lint build

#---------------------------------------------------------------------------

init:
	poetry install

reformat:
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

	version=$$(toml get tool.poetry.version --toml-path pyproject.toml); \
	docker build . \
	    --build-arg VERSION=$${version} \
	    --pull \
	    -t hub.osg-htc.org/brian.aydemir/jupyterhub-monitoring:$${version}

clean:
	rm -rf dist
