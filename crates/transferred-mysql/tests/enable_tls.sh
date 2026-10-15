#!/bin/sh
set -eu
mkdir /tmp/transferred-tls
cd /tmp/transferred-tls
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 -keyout ca.key -out ca.pem -subj /CN=transferred-test-CA -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign
openssl req -newkey rsa:2048 -nodes -sha256 -keyout server.key -out server.csr -subj /CN=transferred-test-server
printf 'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\nkeyUsage=critical,digitalSignature,keyEncipherment\nbasicConstraints=critical,CA:FALSE\n' > extensions
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out server.pem -days 1 -sha256 -extfile extensions
chown -R mysql:mysql /tmp/transferred-tls
exec /usr/local/bin/docker-entrypoint.sh mysqld --ssl-ca=/tmp/transferred-tls/ca.pem --ssl-cert=/tmp/transferred-tls/server.pem --ssl-key=/tmp/transferred-tls/server.key "$@"
