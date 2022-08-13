#!/usr/bin/env bash

# Script that sets up a virtual env in $CF_VENV_PATH.
# For more info on functionality and parameters, see the generic setup script _setup_venv.sh.

action() {
    local this_file="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "$this_file" )" && pwd )"
    local cf_dir="$( dirname "$this_dir" )"

    # set variables and source the generic venv setup
    export CF_VENV_NAME="$( basename "${this_file%.sh}" )"
    export CF_VENV_REQUIREMENTS="$this_dir/ml_tf.txt,$cf_dir/requirements_dev.txt"

    source "$this_dir/_setup_venv.sh" "$@"
}
action "$@"
