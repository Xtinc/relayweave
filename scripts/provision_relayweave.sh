#!/usr/bin/env bash
set -euo pipefail
umask 077

program_name=${0##*/}
role=
case "$program_name" in
relayweave-provision-node) role=node ;;
relayweave-provision-agent) role=agent ;;
relayweave-provision-dashboard) role=dashboard ;;
relayweave-provision-proxy) role=proxy ;;
esac
die() { echo "$*" >&2; exit 1; }
usage() {
    local prefix=$program_name
    [[ -n "$role" ]] || prefix+=" <node|agent|proxy|dashboard>"
    echo "Usage: $prefix <operation> [options]" >&2
    echo "  --install-certs FILE [--cert-dir DEST] [--dry-run]  Install or update a certificate bundle" >&2
    echo "  --remove-certs [--cert-dir DEST] [--dry-run]        Back up and remove certificates" >&2
    echo "  --config FILE [--cert-dir SOURCE] [--start|--restart] [--dry-run]" >&2
    echo "  --verify-certs [--verify-host HOST]...             Opt in to certificate validation using system openssl" >&2
    exit "${1:-2}"
}

if [[ -z "$role" ]]; then
    case "${1:-}" in
    node|agent|proxy|dashboard) role=$1; shift ;;
    -h|--help) usage 0 ;;
    *) usage ;;
    esac
fi
cert_dir=
config_file=
archive=
certificate_action=
service_action=none
dry_run=false
verify_certs=false
verify_hosts=()
while [[ $# -gt 0 ]]; do
    case "$1" in
    --cert-dir|--config|--verify-host|--install-certs)
        [[ $# -ge 2 && -n "$2" ]] || usage
        case "$1" in
        --cert-dir) cert_dir=$2 ;;
        --config) config_file=$2 ;;
        --verify-host) verify_hosts+=("$2"); verify_certs=true ;;
        --install-certs)
            [[ -z "$certificate_action" ]] || usage
            certificate_action=install
            archive=$2
            ;;
        esac
        shift 2
        ;;
    --remove-certs)
        [[ -z "$certificate_action" ]] || usage
        certificate_action=remove
        shift
        ;;
    --start|--restart)
        [[ "$service_action" == none ]] || usage
        service_action=${1#--}
        shift
        ;;
    --dry-run) dry_run=true; shift ;;
    --verify-certs) verify_certs=true; shift ;;
    -h|--help) usage 0 ;;
    *) usage ;;
    esac
done
if [[ -n "$certificate_action" ]]; then
    [[ "$role" == node || "$role" == agent || "$role" == dashboard ]] || usage
    [[ -z "$config_file" && "$service_action" == none ]] || usage
else
    [[ -n "$config_file" ]] || usage
    [[ -s "$config_file" ]] || die "Configuration file does not exist or is empty: $config_file"
    config_file=$(realpath -- "$config_file")
fi
[[ "$role" == node || ${#verify_hosts[@]} == 0 ]] ||
    die "--verify-host applies only to RelayNode certificates."
[[ "$verify_certs" == false || ( "$role" != proxy && "$certificate_action" != remove ) ]] ||
    die "--verify-certs applies only to certificate installation or deployment."

deployed_files=()
destination_cert_dir=/etc/relayweave/certs
destination_config=/etc/relayweave/$role.json
service_name=relayweave-$role.service
file_group=root
config_mode=0644
cert_dir_mode=0755
key_mode=0600
case "$role" in
node) deployed_files=(server-ca.pem client-ca.pem server.pem server.key); identity=server ;;
agent|dashboard)
    deployed_files=(server-ca.pem shared-client.pem shared-client.key)
    identity=shared-client
    if [[ "$role" == dashboard ]]; then
        destination_cert_dir=/etc/relayweave/dashboard-certs
        file_group=relayweave-dashboard
        config_mode=0640
        cert_dir_mode=0750
        key_mode=0640
    fi
    ;;
proxy) [[ -z "$cert_dir" ]] || die "--cert-dir does not apply to RelayWeave Proxy." ;;
esac
required_files=("${deployed_files[@]}")

work_dir=$(mktemp -d)
staging_dir=
backup_dir=
destinations=()
sources=()
modes=()
rollback_count=0
cleanup() {
    local status=$? i destination
    trap - EXIT INT TERM
    set +e
    if ((rollback_count > 0)); then
        for ((i=rollback_count-1; i>=0; i--)); do
            destination=${destinations[$i]}
            if [[ -n "$backup_dir" && -f "$backup_dir/${destination##*/}" ]]; then
                cp -p -- "$backup_dir/${destination##*/}" "$destination" ||
                    echo "Restore failed; restore this file from backup: $destination" >&2
            else
                rm -f -- "$destination"
            fi
        done
        echo "Operation failed; attempted to restore original files. Backup: ${backup_dir:-none}" >&2
    fi
    rm -rf -- "$work_dir"
    [[ -z "$staging_dir" ]] || rm -rf -- "$staging_dir"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

reject_links() {
    local path=$1
    while [[ "$path" != / && "$path" != . ]]; do
        [[ ! -L "$path" ]] || die "Refusing symbolic link destination: $path"
        path=$(dirname -- "$path")
    done
}
check_certificate() {
    local certificate=$1 purpose=$2 start end now
    start=$(openssl x509 -in "$certificate" -noout -startdate)
    end=$(openssl x509 -in "$certificate" -noout -enddate)
    start=$(date -d "${start#notBefore=}" +%s)
    end=$(date -d "${end#notAfter=}" +%s)
    now=$(date +%s)
    ((start <= now && now < end)) || die "Certificate is expired or not yet valid: $certificate"
    [[ "$(openssl x509 -in "$certificate" -purpose -noout)" == *"$purpose : Yes"* ]] ||
        die "Unsuitable certificate purpose: $certificate"
}
node_identity() {
    local subject pattern='(^|,)CN=([A-Za-z0-9][A-Za-z0-9._-]{0,63})(,|$)'
    subject=$(openssl x509 -in "$1" -noout -subject -nameopt RFC2253)
    subject=${subject#subject=}
    [[ "$subject" =~ $pattern ]] || die "Cannot read Node certificate identity: $1"
    echo "${BASH_REMATCH[2]}"
}

if [[ -n "$certificate_action" ]]; then
    destination_cert_dir=${cert_dir:-$destination_cert_dir}
fi
# Keep the spelling until links have been checked; realpath would hide them.
destination_cert_dir=$(realpath -ms -- "$destination_cert_dir")
reject_links "$destination_cert_dir"
if [[ "$certificate_action" == install ]]; then
    [[ -f "$archive" ]] || die "Certificate bundle not found: $archive"
    cp -- "$archive" "$work_dir/input.tar.gz"
    archive=$work_dir/input.tar.gz
    members=$(tar -tzf "$archive") || die "Cannot read certificate bundle."
    [[ "$(printf '%s\n' "$members" | LC_ALL=C sort)" == "$(printf '%s\n' "${required_files[@]}" | LC_ALL=C sort)" ]] ||
        die "Unexpected bundle contents; only the certificates and private key required by $role are allowed."
    types=$(tar -tvzf "$archive") || die "Cannot read certificate bundle."
    while IFS= read -r entry; do
        [[ "$entry" == -* ]] || die "Certificate bundle must contain only regular files."
    done <<< "$types"
    mkdir "$work_dir/certs"
    tar -xzf "$archive" -C "$work_dir/certs" --no-same-owner --no-same-permissions
    cert_dir=$work_dir/certs
fi

if [[ "$certificate_action" != remove && "$role" != proxy ]]; then
    [[ -d "$cert_dir" ]] || die "Certificate directory does not exist: $cert_dir"
    cert_dir=$(realpath -- "$cert_dir")
    for file in "${required_files[@]}"; do
        [[ -s "$cert_dir/$file" && -f "$cert_dir/$file" ]] ||
            die "Required certificate file does not exist or is empty: $cert_dir/$file"
    done
    if [[ "$role" == node && -z "$certificate_action" ]]; then
        command -v relayweave-node >/dev/null ||
            die "relayweave-node is required to validate cluster listener configuration."
        relayweave-node --check-cluster-config "$config_file"
    fi
fi

if [[ "$verify_certs" == true ]]; then
    command -v openssl >/dev/null || die "--verify-certs requires the openssl command."
    certificate_file=$cert_dir/$identity.pem
    private_key_file=$cert_dir/$identity.key
    check_certificate "$cert_dir/server-ca.pem" "SSL server CA"
    purpose="SSL client"
    [[ "$role" != node ]] || purpose="SSL server"
    check_certificate "$certificate_file" "$purpose"
    if [[ "$role" == node ]]; then
        check_certificate "$cert_dir/client-ca.pem" "SSL client CA"
        openssl verify -CAfile "$cert_dir/server-ca.pem" -purpose sslserver "$certificate_file"
        openssl verify -CAfile "$cert_dir/server-ca.pem" -purpose sslclient "$certificate_file"
        for host in "${verify_hosts[@]}"; do
            verify_option=-verify_hostname
            [[ "$host" != *:* && ! "$host" =~ ^[0-9.]+$ ]] || verify_option=-verify_ip
            openssl verify -CAfile "$cert_dir/server-ca.pem" -purpose sslserver \
                "$verify_option" "$host" "$certificate_file"
        done
        if [[ -n "$certificate_action" ]]; then
            incoming_id=$(node_identity "$certificate_file")
            if [[ -e "$destination_cert_dir/server.pem" ]]; then
                reject_links "$destination_cert_dir/server.pem"
                installed_id=$(node_identity "$destination_cert_dir/server.pem")
                [[ "$incoming_id" == "$installed_id" ]] || die "Node identity mismatch; refusing replacement."
            fi
        fi
    elif [[ -f "$cert_dir/client-ca.pem" ]]; then
        openssl verify -CAfile "$cert_dir/client-ca.pem" -purpose sslclient "$certificate_file"
    fi
    certificate_public_key=$(openssl x509 -in "$certificate_file" -pubkey -noout |
        openssl pkey -pubin -outform DER | openssl dgst -sha256)
    private_public_key=$(openssl pkey -in "$private_key_file" -passin pass: -pubout -outform DER |
        openssl dgst -sha256)
    [[ "$certificate_public_key" == "$private_public_key" ]] ||
        die "Certificate and private key do not match: $certificate_file, $private_key_file"
    openssl x509 -in "$certificate_file" -noout -subject -dates -fingerprint -sha256
    [[ "$role" != node ]] || openssl x509 -in "$certificate_file" -noout -ext subjectAltName
fi

if [[ -z "$certificate_action" ]]; then
    destinations+=("$destination_config")
    sources+=("$config_file")
    modes+=("$config_mode")
fi
for file in "${deployed_files[@]}"; do
    destination=$destination_cert_dir/$file
    if [[ "$certificate_action" == remove ]]; then
        # Keep the Node/client shared CA when both identities use this directory.
        other_identity=shared-client
        [[ "$role" == node ]] || other_identity=server
        if [[ "$file" == server-ca.pem && ( -e "$destination_cert_dir/$other_identity.pem" ||
              -L "$destination_cert_dir/$other_identity.pem" ) ]]; then
            echo "Keeping server-ca.pem because the other role uses it."
            continue
        fi
        [[ -e "$destination" || -L "$destination" ]] || continue
    fi
    destinations+=("$destination")
    sources+=("$cert_dir/$file")
    mode=0644
    [[ "$file" != *.key ]] || mode=$key_mode
    modes+=("$mode")
done
for destination in "${destinations[@]}"; do
    reject_links "$destination"
    [[ ! -e "$destination" || -f "$destination" ]] || die "Destination is not a regular file: $destination"
done
echo "RelayWeave $role, action: ${certificate_action:-configuration and certificate deployment}"
printf '  %s\n' "${destinations[@]}"
if [[ "$dry_run" == true ]]; then
    echo "Dry run complete; no files or services were changed."
    exit 0
fi
if [[ ${#destinations[@]} == 0 ]]; then
    echo "No certificates to remove."
    exit 0
fi
[[ $(id -u) == 0 ]] || die "Provisioning must run as root; use --dry-run to validate without installing."
if [[ -z "$certificate_action" && ( "$role" == node || "$role" == agent ) ]]; then
    ambient=$(systemctl show "$service_name" --property=AmbientCapabilities --value)
    bounding=$(systemctl show "$service_name" --property=CapabilityBoundingSet --value)
    [[ " ${ambient,,} " == *" cap_net_raw "* && " ${bounding,,} " == *" cap_net_raw "* ]] ||
        die "$service_name must grant and retain CAP_NET_RAW."
fi
if [[ "$service_action" != none ]]; then
    command -v systemctl >/dev/null || die "systemctl is required for --start or --restart."
fi

if [[ "$certificate_action" != remove ]]; then
    parent=$(dirname -- "${destinations[0]}")
    if [[ -z "$certificate_action" ]]; then
        install -d -m 0755 -- "$parent"
    fi
    if [[ ${#deployed_files[@]} -gt 0 ]]; then
        install -d -m "$cert_dir_mode" -o root -g "$file_group" -- "$destination_cert_dir"
    fi
    staging_dir=$(mktemp -d "$parent/.provision.XXXXXX")
    for i in "${!destinations[@]}"; do
        install -m "${modes[$i]}" -o root -g "$file_group" -- "${sources[$i]}" "$staging_dir/$i"
    done
fi
for destination in "${destinations[@]}"; do
    if [[ -f "$destination" ]]; then
        if [[ -z "$backup_dir" ]]; then
            reject_links /var/backups/relayweave
            install -d -m 0700 /var/backups/relayweave
            backup_dir=$(mktemp -d "/var/backups/relayweave/$role-$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")
        fi
        cp -p -- "$destination" "$backup_dir/"
    fi
done
for i in "${!destinations[@]}"; do
    rollback_count=$((i + 1))
    if [[ "$certificate_action" == remove ]]; then
        rm -- "${destinations[$i]}"
    else
        mv -f -- "$staging_dir/$i" "${destinations[$i]}"
    fi
done
rollback_count=0
echo "Completed: ${certificate_action:-configuration and certificate deployment}"
[[ -z "$backup_dir" ]] || echo "Previous files backed up to: $backup_dir"
case "$service_action" in
start) systemctl enable --now "$service_name" ;;
restart) systemctl try-restart "$service_name" ;;
none)
    echo "Service state is unchanged. Run manually when ready: systemctl restart $service_name"
    ;;
esac
