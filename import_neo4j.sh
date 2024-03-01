#!/usr/bin/env bash

input=$1

if [[ ! -d "$input" ]]; then
    echo "Please specify input directory."
    exit 1
fi

neo4j-admin database import full \
    --multiline-fields=true \
    --nodes=Certificate="$input/cert.vertex.csv" \
    --nodes=Domain="$input/domain.vertex.csv" \
    --nodes=IP="$input/ip.vertex.csv" \
    --relationships=DEPLOYED_ON="$input/deployed_on.relation.csv" \
    --relationships=CONTAINS="$input/contains.relation.csv" \
    --relationships=REDIRECTS="$input/redirects.relation.csv" \
    --relationships=RESOLVES="$input/resolves.relation.csv" \
    --relationships=RETURNS="$input/returns.relation.csv" \
    --relationships=PARENT_DOMAIN="$input/parent_domain.relation.csv" \
    --relationships=SUBDOMAIN_OF="$input/subdomain.relation.csv" \
    --legacy-style-quoting=true \
    --trim-strings=true \
    --id-type=INTEGER \
    neo4j
