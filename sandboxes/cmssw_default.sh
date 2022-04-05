#!/usr/bin/env bash

# Script that sets up a CMSSW environment in $AP_CMSSW_BASE.
# For more info on functionality and parameters, see the generic setup script _setup_cmssw.sh.

action() {
    local this_file="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "$this_file" )" && pwd )"

    # set variables and source the generic CMSSW setup
    export AP_SCRAM_ARCH="slc7_amd64_gcc900"
    export AP_CMSSW_VERSION="CMSSW_12_2_3"
    export AP_CMSSW_ENV_NAME="$( basename "${this_file%.sh}" )"
    export AP_CMSSW_FLAG="1"  # increment when content changed

    source "$this_dir/_setup_cmssw.sh" "$@"
}
action "$@"
