JAVA_HOME ?= $(HOME)/.jdks/temurin-17
export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)
TMPDIR ?= /dev/shm
export TMPDIR
PYTHON ?= .venv/bin/python
export PYTHONPATH := src

.PHONY: setup data clean features analytics ml report test all
setup:
	python3 -m venv .venv
	$(PYTHON) -m pip install --no-cache-dir -r requirements.txt
	$(PYTHON) -m pip install --no-cache-dir -e .
data:
	$(PYTHON) -m mobilite.run_pipeline --stages ingest
clean:
	$(PYTHON) -m mobilite.run_pipeline --stages clean
features:
	$(PYTHON) -m mobilite.run_pipeline --stages features
analytics:
	$(PYTHON) -m mobilite.run_pipeline --stages analytics
ml:
	$(PYTHON) -m mobilite.run_pipeline --stages ml_mode,ml_duration,cluster
report:
	$(PYTHON) -m mobilite.run_pipeline --stages report
test:
	$(PYTHON) -m pytest -q
all:
	$(PYTHON) -m mobilite.run_pipeline --stages all
