#!/usr/bin/env bash
set -euo pipefail
umask 077

die() { echo "$*" >&2; exit 1; }
ask() {
    read -r -p "$1: " REPLY || die "End of input; cancelled."
    REPLY=${REPLY:-${2:-}}
}
yes_no() {
    while true; do
        ask "$1 [y/N]" n
        case "$REPLY" in
        y|Y) return 0 ;;
        n|N) return 1 ;;
        *) echo "Please enter y or n." >&2 ;;
        esac
    done
}
read_days() {
    while true; do
        ask "$1 [$2]" "$2"
        [[ "$REPLY" =~ ^[1-9][0-9]{0,4}$ ]] && ((REPLY <= 36500)) && return
        echo "Please enter an integer from 1 to 36500." >&2
    done
}
read_password() {
    local name=$1 confirmation
    [[ -z "${!name:-}" ]] || return 0
    while true; do
        read -r -s -p "$2: " REPLY || die "End of input; cancelled."
        echo >&2
        [[ -n "$REPLY" ]] || continue
        if [[ "$mode" == 1 ]]; then
            read -r -s -p "Confirm password: " confirmation || die "End of input; cancelled."
            echo >&2
            [[ "$REPLY" == "$confirmation" ]] || { echo "Passwords do not match." >&2; continue; }
        fi
        printf -v "$name" '%s' "$REPLY"
        return
    done
}

valid_ipv4() {
    local part
    local -a octets
    IFS=. read -r -a octets <<< "$1"
    [[ ${#octets[@]} == 4 && "$1" != *. ]] || return 1
    for part in "${octets[@]}"; do
        [[ "$part" =~ ^[0-9]{1,3}$ ]] && ((10#$part <= 255)) || return 1
    done
}
classify_host() {
    local host=$1 address part left right
    local -a groups labels
    if [[ "$host" == *:* ]]; then
        address=$host
        if [[ "$address" == *.* ]]; then
            valid_ipv4 "${address##*:}" || return 1
            address="${address%:*}:0:0"
        fi
        [[ "$address" =~ ^[0-9A-Fa-f:]+$ && "$address" != *:::* ]] || return 1
        if [[ "$address" == *::* ]]; then
            left=${address%%::*}
            right=${address#*::}
            [[ "$right" != *::* && "$left" != *: && "$right" != :* ]] || return 1
            address="${left:+$left:}$right"
            IFS=: read -r -a groups <<< "$address"
            ((${#groups[@]} < 8)) || return 1
        else
            [[ "$address" != :* && "$address" != *: ]] || return 1
            IFS=: read -r -a groups <<< "$address"
            [[ ${#groups[@]} == 8 ]] || return 1
        fi
        for part in "${groups[@]}"; do
            [[ "$part" =~ ^[0-9A-Fa-f]{1,4}$ ]] || return 1
        done
        REPLY="IP:$host"
    elif [[ "$host" =~ ^[0-9.]+$ ]]; then
        valid_ipv4 "$host" || return 1
        REPLY="IP:$host"
    else
        [[ -n "$host" && ${#host} -le 253 && "$host" != *. ]] || return 1
        IFS=. read -r -a labels <<< "$host"
        for part in "${labels[@]}"; do
            [[ "$part" =~ ^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$ ]] || return 1
        done
        REPLY="DNS:$host"
    fi
}

if [[ $# -gt 0 ]]; then
    echo "Usage: bash $0"
    echo "Interactively generate Node/Agent certificate bundles and an offline CA backup; supports CA reuse and renewal."
    [[ $# == 1 && "$1" == --help ]] && exit 0
    exit 2
fi
for tool in openssl tar date sort; do
    command -v "$tool" >/dev/null || die "Required command not found: $tool"
done

while true; do
    ask "1) Create new CAs  2) Reuse a CA backup [1]" 1
    [[ "$REPLY" == 1 || "$REPLY" == 2 ]] && break
done
mode=$REPLY
backup=
renew_client=true
rotate_client=true
if [[ "$mode" == 2 ]]; then
    ask "Path to offline ca-backup.tar.gz"
    [[ -f "$REPLY" ]] || die "CA backup not found."
    backup=$(realpath -- "$REPLY")
    renew_client=false
    rotate_client=false
    if yes_no "Renew the client certificate?"; then
        renew_client=true
        if yes_no "Replace the shared client key? All connected Agents must be updated together."; then
            rotate_client=true
        fi
    fi
fi
default_output="./relayweave-certificates-$(date +%Y%m%d-%H%M%S)"
ask "Output directory (must not exist)[$default_output]" "$default_output"
output_dir=$(realpath -ms -- "$REPLY")
[[ ! -e "$output_dir" && ! -L "$output_dir" ]] || die "Refusing to overwrite existing output directory: $output_dir"
ca_days=3650
if [[ "$mode" == 1 ]]; then
    read_days "CA validity in days" 3650
    ca_days=$REPLY
fi
read_days "New leaf certificate validity in days" 365
leaf_days=$REPLY

node_ids=()
node_hosts=()
node_sans=()
while true; do
    ask "Node ID (leave blank to finish)"
    [[ -n "$REPLY" ]] || break
    node_id=$REPLY
    [[ "$node_id" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ ]] || { echo "Invalid Node ID." >&2; continue; }
    duplicate=false
    for existing in "${node_ids[@]}"; do
        [[ "$existing" != "$node_id" ]] || duplicate=true
    done
    [[ "$duplicate" == false ]] || { echo "Duplicate Node ID." >&2; continue; }
    ask "IP addresses/DNS names for $node_id (comma-separated)"
    hosts_arg=$REPLY
    sans=()
    valid=true
    [[ -n "$hosts_arg" && "$hosts_arg" != ,* && "$hosts_arg" != *, &&
       "$hosts_arg" != *,,* && "$hosts_arg" != *[[:space:]]* ]] || valid=false
    IFS=, read -r -a hosts <<< "$hosts_arg"
    for host in "${hosts[@]}"; do
        classify_host "$host" || { valid=false; break; }
        for existing in "${sans[@]}"; do
            [[ "${existing,,}" != "${REPLY,,}" ]] || valid=false
        done
        sans+=("$REPLY")
    done
    [[ "$valid" == true ]] || { echo "Invalid or duplicate addresses; add this Node again." >&2; continue; }
    node_ids+=("$node_id")
    node_hosts+=("$hosts_arg")
    node_sans+=("$(IFS=,; echo "${sans[*]}")")
done

echo "Output: $output_dir; new certificate validity: $leaf_days days"
for i in "${!node_ids[@]}"; do
    echo "Node ${node_ids[$i]}: ${node_hosts[$i]}"
done
echo "A client bundle and an offline CA backup will also be generated."
yes_no "Proceed with generation?" || exit 0
mkdir -p -- "$(dirname -- "$output_dir")"
work_dir=$(mktemp -d "$(dirname -- "$output_dir")/.generate-certificates.XXXXXX")
cleanup() {
    unset PROXY_SERVER_CA_PASSWORD PROXY_CLIENT_CA_PASSWORD
    rm -rf -- "$work_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
read_password PROXY_SERVER_CA_PASSWORD "Server CA password"
read_password PROXY_CLIENT_CA_PASSWORD "Client CA password"
export PROXY_SERVER_CA_PASSWORD PROXY_CLIENT_CA_PASSWORD
cd "$work_dir"

check_pair() {
    openssl x509 -in "$1" -pubkey -noout > cert-public.pem
    openssl pkey -in "$2" -passin "${3:-pass:}" -pubout > key-public.pem
    cmp -s cert-public.pem key-public.pem || die "Certificate and private key do not match: $1"
}
new_key() {
    local -a options=()
    [[ $# == 1 ]] || options=(-aes-256-cbc -pass "env:$2")
    openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 \
        "${options[@]}" -out "$1" 2>keygen.log || { cat keygen.log >&2; die "Failed to generate private key."; }
}
sign_leaf() {
    local key=$1 certificate=$2 name=$3 ca=$4 password=$5 purpose=$6 san=$7
    openssl req -new -sha256 -batch -key "$key" -out leaf.csr -subj "/CN=$name"
    cat >leaf.ext <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=$purpose
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
subjectAltName=$san
EOF
    openssl x509 -req -in leaf.csr -CA "$ca.pem" -CAkey "$ca.key" \
        -passin "env:$password" -set_serial "0x$(openssl rand -hex 16)" \
        -days "$leaf_days" -sha256 -extfile leaf.ext -out "$certificate"
    check_pair "$certificate" "$key"
}

ca_files=(server-ca.pem server-ca.key client-ca.pem client-ca.key shared-client.pem shared-client.key)
if [[ "$mode" == 2 ]]; then
    cp -- "$backup" ca-backup.tar.gz
    backup=ca-backup.tar.gz
    members=$(tar -tzf "$backup") || die "Cannot read CA backup."
    [[ "$(printf '%s\n' "$members" | LC_ALL=C sort)" == "$(printf '%s\n' "${ca_files[@]}" | LC_ALL=C sort)" ]] ||
        die "Unexpected CA backup contents."
    types=$(tar -tvzf "$backup") || die "Cannot read CA backup."
    while IFS= read -r entry; do
        [[ "$entry" == -* ]] || die "CA backup must contain only regular files."
    done <<< "$types"
    tar -xzf "$backup" --no-same-owner --no-same-permissions
    check_pair shared-client.pem shared-client.key
    openssl verify -no_check_time -CAfile client-ca.pem -purpose sslclient shared-client.pem
else
    for kind in server client; do
        password="PROXY_${kind^^}_CA_PASSWORD"
        new_key "$kind-ca.key" "$password"
        openssl req -x509 -new -sha256 -batch -key "$kind-ca.key" -passin "env:$password" \
            -days "$ca_days" -out "$kind-ca.pem" -subj "/CN=Proxy ${kind^} CA" \
            -addext "basicConstraints=critical,CA:TRUE" \
            -addext "keyUsage=critical,keyCertSign,cRLSign" -addext "subjectKeyIdentifier=hash"
    done
fi
for kind in server client; do
    password="PROXY_${kind^^}_CA_PASSWORD"
    check_pair "$kind-ca.pem" "$kind-ca.key" "env:$password"
    [[ "$(openssl x509 -in "$kind-ca.pem" -noout -ext basicConstraints)" == *CA:TRUE* ]] ||
        die "$kind-ca.pem is not a CA certificate."
    openssl verify -check_ss_sig -CAfile "$kind-ca.pem" "$kind-ca.pem"
    if [[ ( "$kind" == client && "$renew_client" == true ) ||
          ( "$kind" == server && ${#node_ids[@]} -gt 0 ) ]]; then
        openssl x509 -in "$kind-ca.pem" -checkend "$((leaf_days * 86400))" -noout ||
            die "CA expires too soon; shorten the new certificate validity."
    fi
done

mkdir packages nodes
for i in "${!node_ids[@]}"; do
    node_id=${node_ids[$i]}
    node_dir=nodes/$node_id
    mkdir "$node_dir"
    new_key "$node_dir/server.key"
    sign_leaf "$node_dir/server.key" "$node_dir/server.pem" "$node_id" server-ca \
        PROXY_SERVER_CA_PASSWORD serverAuth,clientAuth "${node_sans[$i]}"
    IFS=, read -r -a hosts <<< "${node_hosts[$i]}"
    for host in "${hosts[@]}"; do
        classify_host "$host"
        verify_option=-verify_hostname
        [[ "$REPLY" != IP:* ]] || verify_option=-verify_ip
        openssl verify -CAfile server-ca.pem -purpose sslserver "$verify_option" "$host" "$node_dir/server.pem"
    done
    openssl verify -CAfile server-ca.pem -purpose sslclient "$node_dir/server.pem"
    cp -- server-ca.pem client-ca.pem "$node_dir/"
    chmod 644 "$node_dir/"*.pem
    tar -czf "packages/node-$node_id.tar.gz" -C "$node_dir" -- server.pem server.key server-ca.pem client-ca.pem
done
if [[ "$renew_client" == true ]]; then
    [[ "$rotate_client" == false ]] || new_key shared-client.key
    sign_leaf shared-client.key shared-client.pem "Proxy Shared Client" client-ca \
        PROXY_CLIENT_CA_PASSWORD clientAuth URI:urn:proxy-client:shared
fi
openssl verify -CAfile client-ca.pem -purpose sslclient shared-client.pem ||
    die "Client certificate is invalid; select renewal when reusing a backup."
chmod 644 server-ca.pem client-ca.pem shared-client.pem
chmod 600 server-ca.key client-ca.key shared-client.key
tar -czf packages/client.tar.gz -- shared-client.pem shared-client.key server-ca.pem
tar -czf packages/ca-backup.tar.gz -- "${ca_files[@]}"
chmod 600 packages/*.tar.gz
mv -Tn -- packages "$output_dir"
[[ ! -d packages ]] || die "Output directory was created by another process; refusing to overwrite: $output_dir"
echo "Certificates generated and verified: $output_dir"
echo "Keep ca-backup.tar.gz on the offline signing machine; do not distribute it to Nodes or Agents."
