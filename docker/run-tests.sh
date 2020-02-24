#!/usr/bin/env sh
set -e

chmod 777 source
cd source
pip install --user -r requirements-test.txt
pip install --user -e .

python -m pytest --cov=beam_sink