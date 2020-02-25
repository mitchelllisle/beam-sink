#!/usr/bin/env sh
set -e

cd source
pip install --user -r requirements-test.txt
pip install --user -e .

python -m pytest --cov=beam_sink