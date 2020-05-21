_default: compile

clean:
	rm -fr emcache/_cython/*.c emcache/_cython/*.so build
	find . -name '__pycache__' | xargs rm -rf
	find . -type f -name "*.pyc" -delete

.install-cython:
	pip install Cython==0.29.18
	touch .install-cython

emcache/_cython/cyemcache.c: emcache/_cython/cyemcache.pyx
	cython -3 -o $@ $< -I emcache

cythonize: .install-cython emcache/_cython/cyemcache.c

setup-build:
	python setup.py build_ext --inplace

compile: clean cythonize setup-build

install-dev: compile
	pip install -e ".[dev]"

install: compile
	pip install -e .

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
	python benchmark/sets_gets_stress.py --duration 10 --concurrency 32

install-doc:
	pip install -r docs/requirements.txt

doc:
	make -C docs/ html

.PHONY: clean setup-build install install-dev compile unit test acceptance stress
