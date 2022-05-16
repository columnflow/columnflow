#!/usr/bin/env bash

# Script that installs and sources a virtual environment. Distinctions are made
# depending on whether the installation is already present, and whether the script is called as part
# of a remote (law) job (AP_REMOTE_JOB=1).
#
# Two environment variables are expected to be set before this script is called:
#   - AP_VENV_NAME:
#       The name of the virtual environment. It will be installed into $AP_VENV_PATH/$AP_VENV_NAME.
#   - AP_VENV_REQUIREMENTS:
#       The requirements file containing packages that are installed on top of
#       $AP_BASE/requirements_prod.txt.
#
# Arguments:
# 1. mode: The setup mode. Different values are accepted:
#   - '' (default): The virtual environment is installed when not existing yet and sourced.
#   - reinstall:    The virtual environment is removed first, then reinstalled and sourced.
#
# Note on remote jobs:
# When the AP_REMOTE_JOB variable is found to be "1" (usually set by a remote job bootstrap script),
# no mode is supported and an error is printed when it is set to a non-empty value. In any case, no
# installation will happen but the setup is reused from a pre-compiled software bundle that is
# fetched from a local or remote location and unpacked.

action() {
    local this_file="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "$this_file" )" && pwd )"
    local orig_dir="$( pwd )"


    #
    # get and check arguments
    #

    local mode="${1:-}"
    if [ ! -z "$mode" ] && [ "$mode" != "reinstall" ]; then
        >&2 echo "unknown venv setup mode '$mode'"
        return "1"
    fi
    if [ "$AP_REMOTE_JOB" = "1" ] && [ ! -z "$mode" ]; then
        >&2 echo "the venv setup mode must be empty in remote jobs, but got '$mode'"
        return "2"
    fi


    #
    # check required global variables
    #

    if [ -z "$AP_VENV_NAME" ]; then
        >&2 echo "AP_VENV_NAME is not set but required by $this_file"
        return "3"
    fi
    if [ -z "$AP_VENV_REQUIREMENTS" ]; then
        >&2 echo "AP_VENV_REQUIREMENTS is not set but required by $this_file"
        return "4"
    fi

    # split $AP_VENV_REQUIREMENTS into an array
    local i
    local requirement_files
    IFS="," read -r -a requirement_files <<< "$AP_VENV_REQUIREMENTS"
    for i in "${!requirement_files[@]}"; do
        if [ ! -f "${requirement_files[i]}" ]; then
            >&2 echo "requirement file $i at ${requirement_files[i]} does not exist"
            return "5"
        fi
    done
    local first_requirement_file="${requirement_files[0]}"


    #
    # start the setup
    #

    local install_path="$AP_VENV_PATH/$AP_VENV_NAME"
    local flag_file="$install_path/ap_flag"
    local venv_version="$( cat "$first_requirement_file" | grep -Po "# version \K\d+.*" )"

    # the venv version must be set
    if [ -z "$venv_version" ]; then
        >&2 echo "first requirement file $first_requirement_file does not contain a version line"
        return "6"
    fi

    # remove the current installation
    if [ "$mode" = "reinstall" ]; then
        echo "removing current installation at $install_path (mode '$mode')"
        rm -rf "$install_path"
    fi

    # install or fetch when not existing
    if [ ! -f "$flag_file" ]; then
        if [ "$AP_REMOTE_JOB" = "1" ]; then
            >&2 echo "the venv $AP_VENV_NAME is not installed"
            return "7"
        fi

        echo "installing venv at $install_path"

        rm -rf "$install_path"
        ap_create_venv "$AP_VENV_NAME" || return "$?"

        # activate it
        source "$install_path/bin/activate" "" || return "$?"

        # install requirements
        python3 -m pip install -U pip
        python3 -m pip install -r "$AP_BASE/requirements_prod.txt" || return "$?"
        for i in "${!requirement_files[@]}"; do
            echo -e "\n\x1b[0;49;35minstalling requirement file $i at ${requirement_files[i]}\x1b[0m"
            python3 -m pip install -r "${requirement_files[i]}" || return "$?"
        done
        ap_make_venv_relocateable "$AP_VENV_NAME" || return "$?"

        # write the version into the flag file
        echo "version $venv_version" > "$flag_file"
    else
        # get the current version
        local curr_version="$( cat "$flag_file" | grep -Po "version \K\d+.*" )"
        if [ -z "$curr_version" ]; then
            >&2 echo "the flag file $flag_file does not contain a valid version"
            return "8"
        fi

        # complain when the version is outdated
        if [ "$curr_version" != "$venv_version" ]; then
            >&2 echo ""
            >&2 echo "WARNING: the venv $AP_VENV_NAME located in the directory"
            >&2 echo "WARNING: $install_path"
            >&2 echo "WARNING: seems to be outdated, please consider updating it (mode 'reinstall')"
            >&2 echo ""
        fi

        # activate it
        source "$install_path/bin/activate" "" || return "$?"
    fi

    # mark this as a bash sandbox for law
    export LAW_SANDBOX="bash::\$AP_BASE/sandboxes/$AP_VENV_NAME.sh"
}
action "$@"
