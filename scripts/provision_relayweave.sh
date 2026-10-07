#!/usr/bin/env bash
set -euo pipefail

program_name=${0##*/}
role=
case "$program_name" in
relayweave-provision-node)
    role=node
    ;;
relayweave-provision-agent)
    role=agent
    ;;
relayweave-provision-dashboard)
    role=dashboard
    ;;
relayweave-provision-proxy)
    role=proxy
    ;;
esac

usage() {
    local exit_code=${1:-2}
    if [[ "$role" == proxy ]]; then
        echo "Usage: $program_name --config FILE [--start|--restart] [--dry-run]" >&2
    elif [[ -n "$role" ]]; then
        echo "Usage: $program_name --cert-dir DIR --config FILE [--verify-host HOST]... [--start|--restart] [--dry-run]" >&2
    else
        echo "Usage: $program_name <node|agent|proxy|dashboard> [--cert-dir DIR] --config FILE [--verify-host HOST]... [--start|--restart] [--dry-run]" >&2
    fi
    exit "$exit_code"
}

if [[ -z "$role" ]]; then
    [[ $# -gt 0 ]] || usage
    case "$1" in
    node|agent|proxy|dashboard)
        role=$1
        shift
        ;;
    -h|--help)
        usage 0
        ;;
    *)
        usage
        ;;
    esac
fi

cert_dir=
config_file=
service_action=none
dry_run=false
verify_hosts=()

while [[ $# -gt 0 ]]; do
    case "$1" in
    --cert-dir)
        [[ $# -ge 2 ]] || usage
        cert_dir=$2
        shift 2
        ;;
    --config)
        [[ $# -ge 2 ]] || usage
        config_file=$2
        shift 2
        ;;
    --verify-host)
        [[ $# -ge 2 ]] || usage
        verify_hosts+=("$2")
        shift 2
        ;;
    --start)
        [[ "$service_action" == none ]] || usage
        service_action=start
        shift
        ;;
    --restart)
        [[ "$service_action" == none ]] || usage
        service_action=restart
        shift
        ;;
    --dry-run)
        dry_run=true
        shift
        ;;
    -h|--help)
        usage 0
        ;;
    *)
        usage
        ;;
    esac
done

[[ -n "$config_file" ]] || usage
if [[ "$role" == proxy ]]; then
    [[ -z "$cert_dir" ]] || {
        echo "--cert-dir does not apply to RelayWeave Proxy." >&2
        exit 2
    }
    [[ ${#verify_hosts[@]} -eq 0 ]] || {
        echo "--verify-host applies only to RelayNode certificates." >&2
        exit 2
    }
else
    [[ -n "$cert_dir" ]] || usage
    [[ -d "$cert_dir" ]] || {
        echo "Certificate directory does not exist: $cert_dir" >&2
        exit 1
    }
fi
[[ -s "$config_file" ]] || {
    echo "Configuration file does not exist or is empty: $config_file" >&2
    exit 1
}
config_file=$(cd -- "$(dirname -- "$config_file")" && pwd -P)/$(basename -- "$config_file")
if [[ "$role" != proxy ]]; then
    command -v openssl >/dev/null 2>&1 || {
        echo "OpenSSL was not found in PATH." >&2
        exit 1
    }
    cert_dir=$(cd -- "$cert_dir" && pwd -P)
fi

required_files=()
deployed_files=()
destination_cert_dir=/etc/relayweave/certs
file_group=root
config_mode=0644
cert_dir_mode=0755
key_mode=0600
case "$role" in
node)
    required_files=(server-ca.pem client-ca.pem server.pem server.key)
    deployed_files=(server-ca.pem client-ca.pem server.pem server.key)
    certificate_file=$cert_dir/server.pem
    private_key_file=$cert_dir/server.key
    service_name=relayweave-node.service
    destination_config=/etc/relayweave/node.json
    ;;
agent|dashboard)
    # client-ca.pem is used to verify shared-client.pem but is not deployed to the Agent.
    required_files=(server-ca.pem client-ca.pem shared-client.pem shared-client.key)
    deployed_files=(server-ca.pem shared-client.pem shared-client.key)
    certificate_file=$cert_dir/shared-client.pem
    private_key_file=$cert_dir/shared-client.key
    service_name=relayweave-agent.service
    destination_config=/etc/relayweave/agent.json
    if [[ "$role" == dashboard ]]; then
        service_name=relayweave-dashboard.service
        destination_config=/etc/relayweave/dashboard.json
        destination_cert_dir=/etc/relayweave/dashboard-certs
        file_group=relayweave-dashboard
        config_mode=0640
        cert_dir_mode=0750
        key_mode=0640
    fi
    ;;
proxy)
    service_name=relayweave-proxy.service
    destination_config=/etc/relayweave/proxy.json
    ;;
esac

for file in "${required_files[@]}"; do
    [[ -f "$cert_dir/$file" ]] || {
        echo "Required certificate file does not exist: $cert_dir/$file" >&2
        exit 1
    }
done

if [[ "$role" == node ]]; then
    command -v relayweave-node >/dev/null 2>&1 || {
        echo "relayweave-node is required to validate cluster listener configuration." >&2
        exit 1
    }
    relayweave-node --check-cluster-config "$config_file"
    openssl verify -CAfile "$cert_dir/server-ca.pem" -purpose sslserver "$certificate_file"
    openssl verify -CAfile "$cert_dir/server-ca.pem" -purpose sslclient "$certificate_file"
    for host in "${verify_hosts[@]}"; do
        if [[ "$host" == *:* || "$host" =~ ^[0-9.]+$ ]]; then
            openssl verify -CAfile "$cert_dir/server-ca.pem" -purpose sslserver \
                -verify_ip "$host" "$certificate_file"
        else
            openssl verify -CAfile "$cert_dir/server-ca.pem" -purpose sslserver \
                -verify_hostname "$host" "$certificate_file"
        fi
    done
elif [[ "$role" == agent || "$role" == dashboard ]]; then
    [[ ${#verify_hosts[@]} -eq 0 ]] || {
        echo "--verify-host applies only to RelayNode certificates." >&2
        exit 2
    }
    openssl verify -CAfile "$cert_dir/client-ca.pem" -purpose sslclient "$certificate_file"
fi

if [[ "$role" != proxy ]]; then
    certificate_public_key=$(openssl x509 -in "$certificate_file" -pubkey -noout |
        openssl pkey -pubin -outform DER 2>/dev/null |
        openssl dgst -sha256)
    private_public_key=$(openssl pkey -in "$private_key_file" -pubout -outform DER 2>/dev/null |
        openssl dgst -sha256)
    [[ "$certificate_public_key" == "$private_public_key" ]] || {
        echo "Certificate and private key do not match: $certificate_file, $private_key_file" >&2
        exit 1
    }
fi

if [[ "$role" == proxy ]]; then
    echo "Validated RelayWeave proxy configuration file."
else
    echo "Validated RelayWeave $role certificate inputs and configuration file."
fi
if [[ "$dry_run" == true ]]; then
    echo "Dry run complete; no files or services were changed."
    exit 0
fi

[[ $EUID -eq 0 ]] || {
    echo "Provisioning must run as root; use --dry-run to validate without installing." >&2
    exit 1
}
if [[ "$service_action" != none ]]; then
    command -v systemctl >/dev/null 2>&1 || {
        echo "systemctl is required for --start or --restart." >&2
        exit 1
    }
fi
if [[ "$role" == node || "$role" == agent ]]; then
    command -v systemctl >/dev/null 2>&1 || {
        echo "systemctl is required to verify the $role ICMP capability." >&2
        exit 1
    }
    ambient_capabilities=$(systemctl show "$service_name" --property=AmbientCapabilities --value)
    bounding_capabilities=$(systemctl show "$service_name" --property=CapabilityBoundingSet --value)
    [[ " ${ambient_capabilities,,} " == *" cap_net_raw "* ]] || {
        echo "$service_name must grant CAP_NET_RAW through AmbientCapabilities." >&2
        exit 1
    }
    [[ " ${bounding_capabilities,,} " == *" cap_net_raw "* ]] || {
        echo "$service_name must retain CAP_NET_RAW in CapabilityBoundingSet." >&2
        exit 1
    }
fi

install -d -m 0755 /etc/relayweave
if [[ ${#deployed_files[@]} -gt 0 ]]; then
    install -d -m "$cert_dir_mode" -o root -g "$file_group" "$destination_cert_dir"
fi
staging_dir=$(mktemp -d /etc/relayweave/.provision.XXXXXX)
cleanup() {
    if [[ -n "${staging_dir:-}" && -d "$staging_dir" ]]; then
        rm -rf -- "$staging_dir"
    fi
}
trap cleanup EXIT

install -m "$config_mode" -o root -g "$file_group" "$config_file" "$staging_dir/$(basename -- "$destination_config")"
for file in "${deployed_files[@]}"; do
    mode=0644
    [[ "$file" != *.key ]] || mode=$key_mode
    install -m "$mode" -o root -g "$file_group" "$cert_dir/$file" "$staging_dir/$file"
done

backup_dir=/var/backups/relayweave/${role}-$(date -u +%Y%m%dT%H%M%SZ)-$$
backup_created=false
for destination in "$destination_config"; do
    if [[ -e "$destination" || -L "$destination" ]]; then
        if [[ "$backup_created" == false ]]; then
            install -d -m 0700 "$backup_dir"
            backup_created=true
        fi
        cp -a -- "$destination" "$backup_dir/"
    fi
done
for file in "${deployed_files[@]}"; do
    destination=$destination_cert_dir/$file
    if [[ -e "$destination" || -L "$destination" ]]; then
        if [[ "$backup_created" == false ]]; then
            install -d -m 0700 "$backup_dir"
            backup_created=true
        fi
        cp -a -- "$destination" "$backup_dir/"
    fi
done

mv -f -- "$staging_dir/$(basename -- "$destination_config")" "$destination_config"
for file in "${deployed_files[@]}"; do
    mv -f -- "$staging_dir/$file" "$destination_cert_dir/$file"
done

echo "Installed configuration: $destination_config"
if [[ ${#deployed_files[@]} -gt 0 ]]; then
    echo "Installed certificates: ${deployed_files[*]}"
fi
if [[ "$backup_created" == true ]]; then
    echo "Previous files were backed up to: $backup_dir"
fi

case "$service_action" in
start)
    systemctl enable --now "$service_name"
    ;;
restart)
    systemctl try-restart "$service_name"
    ;;
none)
    echo "Service state was not changed. Start it explicitly when ready: systemctl start $service_name"
    ;;
esac
