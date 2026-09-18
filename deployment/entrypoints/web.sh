#!/bin/bash

set -e

dagster-webserver -h 0.0.0.0 -p 3001 -w workspace.yaml
