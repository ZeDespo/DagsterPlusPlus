#!/bin/bash

set -e

dagster api grpc -h 0.0.0.0 -p 4000 -f dagster_plus_plus/definitions.py
