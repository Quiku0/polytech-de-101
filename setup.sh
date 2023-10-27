#!/bin/bash

mkdir .venvs
python3 -m venv .venvs/dagster
source .venvs/dagster/bin/activate

pip install dagster dagster-webserver

pip install -e ".[dev]"
