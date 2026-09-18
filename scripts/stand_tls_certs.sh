#!/usr/bin/env bash
# The certificate chain the test stand's TLS listener uses: an authority, a server certificate for
# 127.0.0.1, and a client certificate signed by the same authority, so one connection exercises
# both directions of verification. Nothing here is a secret and nothing here is committed: a
# certificate has a lifetime, and a repository is the wrong place to keep one.
#
# Usage: scripts/stand_tls_certs.sh [directory]   (default: .stand-tls next to this repository)
set -euo pipefail

dir="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.stand-tls}"
mkdir -p "$dir"

# The stand is started often and a chain lasts three months, so this is a no-op until it is a day
# from expiring.
if openssl x509 -in "$dir/server.crt" -checkend 86400 -noout >/dev/null 2>&1; then
    exit 0
fi

openssl req -x509 -newkey rsa:2048 -nodes -days 90 -subj "/CN=ruststream-mqtt-stand" \
    -keyout "$dir/ca.key" -out "$dir/ca.crt" 2>/dev/null

for name in server client; do
    openssl req -newkey rsa:2048 -nodes -subj "/CN=$name" \
        -keyout "$dir/$name.key" -out "$dir/$name.csr" 2>/dev/null
    # The client connects by address rather than by name, so the address is what the certificate
    # has to cover: a certificate naming only a host name verifies nothing here.
    openssl x509 -req -in "$dir/$name.csr" -CA "$dir/ca.crt" -CAkey "$dir/ca.key" \
        -CAcreateserial -days 90 -out "$dir/$name.crt" \
        -extfile <(printf 'basicConstraints=CA:FALSE\nsubjectAltName=IP:127.0.0.1\n') 2>/dev/null
done

# The broker reads its key after dropping privileges, so the mode has to let it.
chmod 644 "$dir"/*.key
