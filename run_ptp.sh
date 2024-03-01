#!/usr/bin/env bash

set -eux

docker-compose build ptp

docker-compose up --exit-code-from ptp ptp

docker-compose down --volumes
