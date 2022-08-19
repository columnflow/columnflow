#!/usr/bin/env bash

# Script that installs and sources a virtual environment. Distinctions are made depending on whether
# the venv is already present, and whether the script is called as part of a remote (law) job
# (CF_REMOTE_JOB=1).
#
# Three environment variables are expected to be set before this script is called:
#   CF_VENV_BASE
#       The base directory where virtual environments will be installed. Set by the main setup.sh.
#   CF_VENV_NAME
#       The name of the virtual environment. It will be installed into $CF_VENV_BASE/$CF_VENV_NAME.
#   CF_VENV_REQUIREMENTS
#       The requirements file containing packages that are installed on top of
#       $CF_BASE/requirements_prod.txt.
#
# Upon venv activation, two environment variables are set in addition to those exported by the venv:
#   CF_DEV
#       Set to "1" when CF_VENV_NAME ends with "_dev", and "0" otherwise.
#   LAW_SANDBOX
#       Set to the name of the sandbox to be correctly interpreted later on by law.
#
# Optional arguments:
# 1. mode
#      The setup mode. Different values are accepted:
#        - '' (default): The virtual environment is installed when not existing yet and sourced.
#        - reinstall:    The virtual environment is removed first, then reinstalled and sourced.
# 2. nocheck
#      When "1", the version check at the end of the setup is silenced. However, the exit code of
#      _this_ script might still reflect whether the check was successful or not.
#
# Note on remote jobs:
# When the CF_REMOTE_JOB variable is found to be "1" (usually set by a remote job bootstrap script),
# no mode is supported and an error is printed when it is set to a non-empty value. In any case, no
# installation will happen but the setup is reused from a pre-compiled software bundle that is
# fetched from a local or remote location and unpacked.

action() {
    local shell_is_zsh=$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local orig_dir="${PWD}"


    #
    # get and check arguments
    #

    local mode="${1:-}"
    local nocheck="${2:-}"
    if [ ! -z "${mode}" ] && [ "${mode}" != "reinstall" ]; then
        >&2 echo "unknown venv setup mode '${mode}'"
        return "1"
    fi
    if [ "${CF_REMOTE_JOB}" = "1" ] && [ ! -z "${mode}" ]; then
        >&2 echo "the venv setup mode must be empty in remote jobs, but got '${mode}'"
        return "2"
    fi


    #
    # check required global variables
    #

    if [ -z "${CF_VENV_NAME}" ]; then
        >&2 echo "CF_VENV_NAME is not set but required by ${this_file}"
        return "3"
    fi
    if [ -z "${CF_VENV_REQUIREMENTS}" ]; then
        >&2 echo "CF_VENV_REQUIREMENTS is not set but required by ${this_file}"
        return "4"
    fi

    # split $CF_VENV_REQUIREMENTS into an array
    local requirement_files
    local requirement_files_contains_prod="false"
    if ${shell_is_zsh}; then
        requirement_files=(${(@s:,:)CF_VENV_REQUIREMENTS})
    else
        IFS="," read -r -a requirement_files <<< "${CF_VENV_REQUIREMENTS}"
    fi
    for f in ${parts[@]}; do
        if [ ! -f "${f}" ]; then
            >&2 echo "requirement file '${f}' does not exist"
            return "5"
        fi
        if [ "${f}" = "${CF_BASE}/requirements_prod.txt" ]; then
            requirement_files_contains_prod="true"
        fi
    done
    local i0="$( ${shell_is_zsh} && echo "1" || echo "0" )"
    local first_requirement_file="${requirement_files[$i0]}"

    #
    # start the setup
    #

    local install_path="${CF_VENV_BASE}/${CF_VENV_NAME}"
    local flag_file="${install_path}/cf_flag"
    local pending_flag_file="${CF_VENV_BASE}/pending_${CF_VENV_NAME}"
    local venv_version="$( cat "${first_requirement_file}" | grep -Po "# version \K\d+.*" )"

    # the venv version must be set
    if [ -z "${venv_version}" ]; then
        >&2 echo "first requirement file ${first_requirement_file} does not contain a version line"
        return "6"
    fi

    # ensure the CF_VENV_BASE exists
    mkdir -p "${CF_VENV_BASE}"

    # remove the current installation
    if [ "${mode}" = "reinstall" ]; then
        echo "removing current installation at $install_path (mode '${mode}')"
        rm -rf "${install_path}"
    fi

    # complain in remote jobs when the env is not installed
    if [ "${CF_REMOTE_JOB}" = "1" ] && [ ! -f "${flag_file}" ]; then
        >&2 echo "the venv ${CF_VENV_NAME} is not installed"
        return "7"
    fi

    # from here onwards, files and directories could be created and in order to prevent race
    # conditions from multiple processes, guard the setup with the pending_flag_file and sleep for a
    # random amount of seconds between 0 and 10 to further reduce the chance of simultaneously
    # starting processes getting here at the same time
    if [ ! -f "${flag_file}" ]; then
        local sleep_counter="0"
        sleep "$( python3 -c 'import random;print(random.random() * 10)')"
        while [ -f "${pending_flag_file}" ]; do
            # wait at most 15 minutes
            sleep_counter="$(( $sleep_counter + 1 ))"
            if [ "${sleep_counter}" -ge 180 ]; then
                >&2 echo "venv ${CF_VENV_NAME} is setup in different process, but number of sleeps exceeded"
                return "8"
            fi
            echo -e "\x1b[0;49;36mvenv ${CF_VENV_NAME} already being setup in different process, sleep ${sleep_counter} / 180\x1b[0m"
            sleep 5
        done
    fi

    # possible return value
    local ret="0"

    # install or fetch when not existing
    if [ ! -f "${flag_file}" ]; then
        touch "${pending_flag_file}"
        echo "installing venv at ${install_path}"

        rm -rf "${install_path}"
        cf_create_venv "${CF_VENV_NAME}" || ( rm -f "${pending_flag_file}" && return "9" )

        # activate it
        source "${install_path}/bin/activate" "" || ( rm -f "${pending_flag_file}" && return "10" )

        # update pip
        echo -e "\n\x1b[0;49;35mupdating pip\x1b[0m"
        python3 -m pip install -U pip || ( rm -f "${pending_flag_file}" && return "11" )

        # install basic production requirements
        if ! $requirement_files_contains_prod; then
            echo -e "\n\x1b[0;49;35minstalling requirement file ${CF_BASE}/requirements_prod.txt\x1b[0m"
            python3 -m pip install -r "${CF_BASE}/requirements_prod.txt" || ( rm -f "${pending_flag_file}" && return "12" )
        fi

        # install requirement files
        for i in "${!requirement_files[@]}"; do
            echo -e "\n\x1b[0;49;35minstalling requirement file $i at ${requirement_files[i]}\x1b[0m"
            python3 -m pip install -r "${requirement_files[i]}" || ( rm -f "${pending_flag_file}" && return "13" )
        done

        # ensure that the venv is relocateable
        cf_make_venv_relocateable "${CF_VENV_NAME}" || ( rm -f "${pending_flag_file}" && return "14" )

        # write the version and a timestamp into the flag file
        echo "version ${venv_version}" > "${flag_file}"
        echo "timestamp $( date "+%s" )" >> "${flag_file}"
        rm -f "${pending_flag_file}"
    else
        # get the current version
        local curr_version="$( cat "${flag_file}" | grep -Po "version \K\d+.*" )"
        if [ -z "${curr_version}" ]; then
            >&2 echo "the flag file ${flag_file} does not contain a valid version"
            return "20"
        fi

        # complain when the version is outdated
        if [ "${curr_version}" != "${venv_version}" ]; then
            ret="21"
            if [ "${nocheck}" != "1" ]; then
                >&2 echo ""
                >&2 echo "WARNING: outdated venv '${CF_VENV_NAME}' located at"
                >&2 echo "WARNING: ${install_path}"
                >&2 echo "WARNING: please consider updating it by adding 'reinstall' to the source command"
                >&2 echo ""
            fi
        fi

        # activate it
        source "${install_path}/bin/activate" "" || return "$?"
    fi

    # export variables
    export CF_VENV_NAME="${CF_VENV_NAME}"
    export CF_DEV="$( [[ "${CF_VENV_NAME}" == *_dev ]] && echo "1" || echo "0" )"

    # mark this as a bash sandbox for law
    export LAW_SANDBOX="bash::\$CF_BASE/sandboxes/${CF_VENV_NAME}.sh"

    return "${ret}"
}
action "$@"
