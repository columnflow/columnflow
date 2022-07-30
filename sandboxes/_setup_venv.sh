#!/usr/bin/env bash

# Script that installs and sources a virtual environment. Distinctions are made depending on whether
# the venv is already present, and whether the script is called as part of a remote (law) job
# (AP_REMOTE_JOB=1).
#
# Two environment variables are expected to be set before this script is called:
#   - AP_VENV_NAME:
#       The name of the virtual environment. It will be installed into $AP_VENV_PATH/$AP_VENV_NAME.
#   - AP_VENV_REQUIREMENTS:
#       The requirements file containing packages that are installed on top of
#       $AP_BASE/requirements_prod.txt.
#
# Upon venv activation, two environment variables are set in addition to those exported by the venv
# itself:
#   - AP_DEV:
#       Set to "1" when AP_VENV_NAME ends with "_dev", and "0" otherwise.
#   - LAW_SANDBOX:
#       Set to the name of the sandbox to be correctly interpreted later on by law.
#
# Optional arguments:
# 1. mode:
#      The setup mode. Different values are accepted:
#        - '' (default): The virtual environment is installed when not existing yet and sourced.
#        - reinstall:    The virtual environment is removed first, then reinstalled and sourced.
# 2. nocheck:
#      When "1", the version check at the end of the setup is silenced. However, the exit code of
#      _this_ script might still reflect whether the check was successful or not.
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
    local nocheck="${2:-}"
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
    local pending_flag_file="$AP_VENV_PATH/pending_${AP_VENV_NAME}"
    local venv_version="$( cat "$first_requirement_file" | grep -Po "# version \K\d+.*" )"

    # the venv version must be set
    if [ -z "$venv_version" ]; then
        >&2 echo "first requirement file $first_requirement_file does not contain a version line"
        return "6"
    fi

    # ensure the AP_VENV_PATH exists
    mkdir -p "$AP_VENV_PATH"

    # remove the current installation
    if [ "$mode" = "reinstall" ]; then
        echo "removing current installation at $install_path (mode '$mode')"
        rm -rf "$install_path"
    fi

    # complain in remote jobs when the env is not installed
    if [ "$AP_REMOTE_JOB" = "1" ] && [ ! -f "$flag_file" ]; then
        >&2 echo "the venv $AP_VENV_NAME is not installed"
        return "7"
    fi

    # from here onwards, files and directories could be created and in order to prevent race
    # conditions from multiple processes, guard the setup with the pending_flag_file and sleep for a
    # random amount of seconds between 0 and 10 to further reduce the chance of simultaneously
    # starting processes getting here at the same time
    if [ ! -f "$flag_file" ]; then
        local sleep_counter="0"
        sleep "$( python3 -c 'import random;print(random.random() * 10)')"
        while [ -f "$pending_flag_file" ]; do
            # wait at most 15 minutes
            sleep_counter="$(( $sleep_counter + 1 ))"
            if [ "$sleep_counter" -ge 180 ]; then
                2>&1 echo "venv $AP_VENV_NAME is setup in different process, but number of sleeps exceeded"
                return "8"
            fi
            echo -e "\x1b[0;49;36mvenv $AP_VENV_NAME already being setup in different process, sleep $sleep_counter / 180\x1b[0m"
            sleep 5
        done
    fi

    # possible return value
    local ret="0"

    # install or fetch when not existing
    if [ ! -f "$flag_file" ]; then
        touch "$pending_flag_file"
        echo "installing venv at $install_path"

        rm -rf "$install_path"
        ap_create_venv "$AP_VENV_NAME" || ( rm -f "$pending_flag_file" && return "9" )

        # activate it
        source "$install_path/bin/activate" "" || ( rm -f "$pending_flag_file" && return "10" )

        # install requirements
        python3 -m pip install -U pip || ( rm -f "$pending_flag_file" && return "11" )
        python3 -m pip install -r "$AP_BASE/requirements_prod.txt" || ( rm -f "$pending_flag_file" && return "12" )

        for i in "${!requirement_files[@]}"; do
            echo -e "\n\x1b[0;49;35minstalling requirement file $i at ${requirement_files[i]}\x1b[0m"
            python3 -m pip install -r "${requirement_files[i]}" || ( rm -f "$pending_flag_file" && return "13" )
        done

        ap_make_venv_relocateable "$AP_VENV_NAME" || ( rm -f "$pending_flag_file" && return "14" )

        # write the version and a timestamp into the flag file
        echo "version $venv_version" > "$flag_file"
        echo "timestamp $( date "+%s" )" >> "$flag_file"
        rm -f "$pending_flag_file"
    else
        # get the current version
        local curr_version="$( cat "$flag_file" | grep -Po "version \K\d+.*" )"
        if [ -z "$curr_version" ]; then
            >&2 echo "the flag file $flag_file does not contain a valid version"
            return "20"
        fi

        # complain when the version is outdated
        if [ "$curr_version" != "$venv_version" ]; then
            ret="21"
            if [ "$nocheck" != "1" ]; then
                >&2 echo ""
                >&2 echo "WARNING: outdated venv '$AP_VENV_NAME' located at"
                >&2 echo "WARNING: $install_path"
                >&2 echo "WARNING: please consider updating it by adding 'reinstall' to the source command"
                >&2 echo ""
            fi
        fi

        # activate it
        source "$install_path/bin/activate" "" || return "$?"
    fi

    # export variables
    export AP_VENV_NAME="$AP_VENV_NAME"
    export AP_DEV="$( [[ "$AP_VENV_NAME" == *_dev ]] && echo "1" || echo "0" )"

    # mark this as a bash sandbox for law
    export LAW_SANDBOX="bash::\$AP_BASE/sandboxes/$AP_VENV_NAME.sh"

    return "$ret"
}
action "$@"
