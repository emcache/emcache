PYTHON ?= python
PIP ?= pip
CYTHON ?= cython

_default: compile

clean:
	rm -fr emcache/_cython/*.c emcache/_cython/*.so build dist
	find . -name '__pycache__' | xargs rm -rf
	find . -type f -name "*.pyc" -delete

setup-build:
	$(PYTHON) setup.py build_ext --inplace

compile: clean setup-build

install-dev:
	$(PIP) install -e '.[dev]'

install:
	$(PIP) install -e .

format:
	isort .
	black .

lint:
	isort --check .
	black --check .
	flake8 .

acceptance:
	pytest -sv tests/acceptance

unit:
	pytest -sv tests/unit

test: 
	pytest -sv tests

coverage:
	coverage run -m pytest -v tests/ --junitxml=build/test.xml
	coverage xml -i -o build/coverage.xml
	coverage report

stress:
	$(PYTHON) benchmark/sets_gets_stress.py --duration 10 --concurrency 32

install-doc:
	$(PIP) install -r docs/requirements.txt

doc:
	make -C docs/ html


.PHONY: clean setup-build install install-dev compile unit test acceptance stress
