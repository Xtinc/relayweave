#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "Usage: bash $0 <output-directory> <node_id=host[,host...]...>" >&2
    echo "Example: bash $0 /secure/proxy-certificates master-1=203.0.113.10 slave-1=203.0.113.11" >&2
    echo "Example: bash $0 /secure/proxy-certificates master-1=proxy.example.com,203.0.113.10" >&2
    exit 2
}

[[ $# -ge 2 ]] || usage

output_dir=$1
shift
node_ids=()
node_hosts=()
node_sans=()

classify_host() {
    local host=$1 part
    if [[ "$host" == *:* ]]; then
        [[ "$host" =~ ^[0-9A-Fa-f:.]+$ ]] || return 1
        REPLY="IP:$host"
        return
    fi
    if [[ "$host" =~ ^[0-9.]+$ ]]; then
        local -a octets
        IFS='.' read -r -a octets <<< "$host"
        [[ ${#octets[@]} -eq 4 ]] || return 1
        for part in "${octets[@]}"; do
            [[ "$part" =~ ^[0-9]{1,3}$ ]] || return 1
            ((10#$part <= 255)) || return 1
        done
        REPLY="IP:$host"
        return
    fi
    [[ ${#host} -le 253 ]] || return 1
    local -a labels
    IFS='.' read -r -a labels <<< "$host"
    for part in "${labels[@]}"; do
        [[ ${#part} -ge 1 && ${#part} -le 63 && "$part" =~ ^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$ ]] || return 1
    done
    REPLY="DNS:$host"
}

for spec in "$@"; do
    [[ "$spec" == *=* && "$spec" != *=*=* ]] || {
        echo "Invalid node specification: $spec" >&2
        usage
    }
    node_id=${spec%%=*}
    hosts_arg=${spec#*=}
    [[ "$node_id" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ ]] || {
        echo "Invalid node_id: $node_id" >&2
        exit 2
    }
    [[ -n "$hosts_arg" && "$hosts_arg" != ,* && "$hosts_arg" != *, &&
       "$hosts_arg" != *,,* && "$hosts_arg" != *[[:space:]]* ]] || {
        echo "Invalid host list for node $node_id: $hosts_arg" >&2
        exit 2
    }
    for existing in "${node_ids[@]}"; do
        [[ "$existing" != "$node_id" ]] || {
            echo "Duplicate node_id: $node_id" >&2
            exit 2
        }
    done

    IFS=',' read -r -a hosts <<< "$hosts_arg"
    sans=()
    for host in "${hosts[@]}"; do
        classify_host "$host" || {
            echo "Invalid IP address or DNS name for node $node_id: $host" >&2
            exit 2
        }
        for existing in "${sans[@]}"; do
            [[ "${existing,,}" != "${REPLY,,}" ]] || {
                echo "Duplicate host for node $node_id: $host" >&2
                exit 2
            }
        done
        sans+=("$REPLY")
    done
    node_ids+=("$node_id")
    node_hosts+=("$hosts_arg")
    node_sans+=("$(IFS=,; echo "${sans[*]}")")
done

command -v openssl >/dev/null 2>&1 || {
    echo "OpenSSL was not found in PATH." >&2
    exit 1
}

generated_entries=(server-ca.pem server-ca.key client-ca.pem client-ca.key shared-client.pem shared-client.key nodes agent)
mkdir -p -- "$output_dir"
output_dir=$(cd "$output_dir" && pwd -P)
for entry in "${generated_entries[@]}"; do
    if [[ -e "$output_dir/$entry" || -L "$output_dir/$entry" ]]; then
        echo "Refusing to overwrite existing entry: $output_dir/$entry" >&2
        echo "Choose a new output directory or remove the old certificate set first." >&2
        exit 1
    fi
done

work_dir=$(mktemp -d "$output_dir/.generate-certificates.XXXXXX")
cleanup() {
    unset PROXY_SERVER_CA_PASSWORD PROXY_CLIENT_CA_PASSWORD
    if [[ -n "${work_dir:-}" && -d "$work_dir" ]]; then
        rm -rf -- "$work_dir"
    fi
}
trap cleanup EXIT
umask 077

read_password() {
    local variable_name=$1 prompt=$2 password confirmation
    [[ -z "${!variable_name:-}" ]] || return 0
    while true; do
        read -r -s -p "$prompt: " password
        echo
        read -r -s -p "Confirm $prompt: " confirmation
        echo
        if [[ -z "$password" ]]; then
            echo "Password must not be empty." >&2
        elif [[ "$password" != "$confirmation" ]]; then
            echo "Passwords do not match; try again." >&2
        else
            printf -v "$variable_name" '%s' "$password"
            return
        fi
    done
}

read_password PROXY_SERVER_CA_PASSWORD "Server CA password"
read_password PROXY_CLIENT_CA_PASSWORD "Client CA password"
export PROXY_SERVER_CA_PASSWORD PROXY_CLIENT_CA_PASSWORD
cd "$work_dir"

cat >client-ext.cnf <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
subjectAltName=URI:urn:proxy-client:shared
EOF

echo "Generating Server CA..."
openssl genpkey -algorithm RSA -aes-256-cbc -pass env:PROXY_SERVER_CA_PASSWORD \
    -pkeyopt rsa_keygen_bits:3072 -out server-ca.key
openssl req -x509 -new -sha256 -batch -key server-ca.key -passin env:PROXY_SERVER_CA_PASSWORD \
    -days 3650 -out server-ca.pem -subj "/CN=Proxy Server CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -addext "subjectKeyIdentifier=hash"

mkdir nodes
for index in "${!node_ids[@]}"; do
    node_id=${node_ids[$index]}
    node_dir="nodes/$node_id"
    mkdir "$node_dir"
    cat >node-ext.cnf <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
subjectAltName=${node_sans[$index]}
EOF
    echo "Generating node certificate: $node_id (${node_hosts[$index]})..."
    openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out "$node_dir/server.key"
    openssl req -new -sha256 -batch -key "$node_dir/server.key" -out node.csr -subj "/CN=$node_id"
    openssl x509 -req -in node.csr -CA server-ca.pem -CAkey server-ca.key \
        -passin env:PROXY_SERVER_CA_PASSWORD -CAcreateserial \
        -out "$node_dir/server.pem" -days 365 -sha256 -extfile node-ext.cnf
done
unset PROXY_SERVER_CA_PASSWORD

echo "Generating Client CA and shared client certificate..."
openssl genpkey -algorithm RSA -aes-256-cbc -pass env:PROXY_CLIENT_CA_PASSWORD \
    -pkeyopt rsa_keygen_bits:3072 -out client-ca.key
openssl req -x509 -new -sha256 -batch -key client-ca.key -passin env:PROXY_CLIENT_CA_PASSWORD \
    -days 3650 -out client-ca.pem -subj "/CN=Proxy Client CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -addext "subjectKeyIdentifier=hash"
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out shared-client.key
openssl req -new -sha256 -batch -key shared-client.key -out shared-client.csr -subj "/CN=Proxy Shared Client"
openssl x509 -req -in shared-client.csr -CA client-ca.pem -CAkey client-ca.key \
    -passin env:PROXY_CLIENT_CA_PASSWORD -CAcreateserial \
    -out shared-client.pem -days 365 -sha256 -extfile client-ext.cnf
unset PROXY_CLIENT_CA_PASSWORD

echo "Verifying certificate chains, usages, identities, and private keys..."
for index in "${!node_ids[@]}"; do
    node_id=${node_ids[$index]}
    node_dir="nodes/$node_id"
    IFS=',' read -r -a hosts <<< "${node_hosts[$index]}"
    for host in "${hosts[@]}"; do
        if [[ "$host" == *:* || "$host" =~ ^[0-9.]+$ ]]; then
            openssl verify -CAfile server-ca.pem -purpose sslserver -verify_ip "$host" "$node_dir/server.pem"
        else
            openssl verify -CAfile server-ca.pem -purpose sslserver -verify_hostname "$host" "$node_dir/server.pem"
        fi
    done
    openssl verify -CAfile server-ca.pem -purpose sslclient "$node_dir/server.pem"
    cmp -s <(openssl x509 -in "$node_dir/server.pem" -pubkey -noout) \
        <(openssl pkey -in "$node_dir/server.key" -pubout) || {
        echo "Generated certificate and key do not match for node: $node_id" >&2
        exit 1
    }
    cp -- server-ca.pem client-ca.pem "$node_dir/"
    chmod 600 "$node_dir/server.key"
    chmod 644 "$node_dir/server.pem" "$node_dir/server-ca.pem" "$node_dir/client-ca.pem"
done

openssl verify -CAfile client-ca.pem -purpose sslclient shared-client.pem
cmp -s <(openssl x509 -in shared-client.pem -pubkey -noout) \
    <(openssl pkey -in shared-client.key -pubout) || {
    echo "Generated client certificate and key do not match." >&2
    exit 1
}

mkdir agent
cp -- server-ca.pem client-ca.pem shared-client.pem shared-client.key agent/
chmod 600 agent/shared-client.key
chmod 644 agent/server-ca.pem agent/client-ca.pem agent/shared-client.pem

chmod 600 server-ca.key client-ca.key shared-client.key
chmod 644 server-ca.pem client-ca.pem shared-client.pem
for entry in "${generated_entries[@]}"; do
    mv -- "$entry" "$output_dir/$entry"
done

echo
echo "Certificates generated and verified in: $output_dir"
for node_id in "${node_ids[@]}"; do
    echo "Node $node_id deploys: nodes/$node_id/{client-ca.pem,server-ca.pem,server.pem,server.key}"
done
echo "Agents deploy from: agent/{server-ca.pem,shared-client.pem,shared-client.key}"
echo "Keep server-ca.key and client-ca.key offline."
