#!/usr/bin/env sh
pip install -r requirements-test.txt

pytest --cov-report=xml > coverage.xml