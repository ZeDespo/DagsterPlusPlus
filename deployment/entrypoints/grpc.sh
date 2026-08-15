#!/bin/bash

set -e

dagster api grpc -h 0.0.0.0 -p 4000 -f sip/repo.py
