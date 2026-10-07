#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
source_dir=$(cd -- "$script_dir/.." && pwd -P)
build_dir=${1:-"$source_dir/build-package"}
package_release=${RELAYWEAVE_PACKAGE_RELEASE:-1}
build_jobs=${RELAYWEAVE_BUILD_JOBS:-1}

if [[ ! "$build_jobs" =~ ^[1-9][0-9]*$ ]]; then
    echo "RELAYWEAVE_BUILD_JOBS must be a positive integer" >&2
    exit 2
fi

cmake -S "$source_dir" -B "$build_dir" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="--param=ggc-min-expand=20 --param=ggc-min-heapsize=65536" \
    -DBUILD_TESTING=OFF \
    -DRELAYWEAVE_PACKAGE_RELEASE="$package_release"
cmake --build "$build_dir" --parallel "$build_jobs"
cpack --config "$build_dir/CPackConfig.cmake" -G DEB

echo "Packages written to $build_dir/packages"
