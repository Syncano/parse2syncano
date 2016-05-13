#!/bin/bash

set -e

flake8 .
isort --recursive --check-only .
