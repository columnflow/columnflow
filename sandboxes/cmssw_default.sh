#!/usr/bin/env bash

# Script that sets up a CMSSW environment in $CF_CMSSW_BASE.
# For more info on functionality and parameters, see the generic setup script _setup_cmssw.sh.

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # get the os version
    local os_version="$( cat /etc/os-release | grep VERSION_ID | sed -E 's/VERSION_ID="([0-9]+)(|\..*)"/\1/' )"

    # set variables and source the generic CMSSW setup
    export CF_SANDBOX_FILE="${CF_SANDBOX_FILE:-${this_file}}"
    export CF_SCRAM_ARCH="el9_amd64_gcc11"
    export CF_CMSSW_VERSION="CMSSW_13_0_19"
    export CF_CMSSW_ENV_NAME="$( basename "${this_file%.sh}" )"
    export CF_CMSSW_FLAG="1"  # increment when content changed

    source "${this_dir}/_setup_cmssw.sh" "$@"
}
action "$@"
