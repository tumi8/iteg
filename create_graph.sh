#!/usr/bin/env bash

set -eux

docker-compose build parser

docker-compose up --exit-code-from parser parser

docker-compose down --volumes
