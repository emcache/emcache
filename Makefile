PYTHON ?= python
PIP ?= pip
CYTHON ?= cython

_default: install

clean:
	rm -fr emcache/_cython/*.c emcache/_cython/*.so build dist
	find . -name '__pycache__' | xargs rm -rf
	find . -type f -name "*.pyc" -delete

install-dev: clean
	$(PIP) install Cython==0.29.21
	$(PIP) install -e ".[dev]"

install:
	$(PIP) install Cython==0.29.21
	$(PIP) install -e .

format:
	isort --recursive .
	black .

lint:
	isort --check-only --recursive .
	black --check .
	flake8

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


.PHONY: clean install install-dev unit test acceptance stress
