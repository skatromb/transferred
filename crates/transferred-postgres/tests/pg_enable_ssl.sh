#!/bin/sh
# Container entrypoint for the TLS tests: self-sign a certificate, then hand control back to the
# stock one with TLS switched on. Runs as root, before `docker-entrypoint.sh` drops to `postgres`.
set -e

openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost \
    -keyout /tmp/server.key -out /tmp/server.crt

chown postgres /tmp/server.key /tmp/server.crt
chmod 600 /tmp/server.key

exec docker-entrypoint.sh postgres \
    -c ssl=on \
    -c ssl_cert_file=/tmp/server.crt \
    -c ssl_key_file=/tmp/server.key
