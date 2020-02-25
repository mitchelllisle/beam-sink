#!/usr/bin/env sh
set -e

cd source
sudo pip install --user -r requirements-test.txt
sudo pip install --user -e .

python -m pytest --cov=beam_sink --cov-report=xml > coverage.xml