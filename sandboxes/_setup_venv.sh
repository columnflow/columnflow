#!/usr/bin/env bash

# Script that installs and sources a virtual environment. Distinctions are made depending on whether
# the venv is already present, and whether the script is called as part of a remote (law) job
# (CF_REMOTE_ENV=1).
#
# Four environment variables are expected to be set before this script is called:
#   CF_SANDBOX_FILE
#       The path of the file that contained the sandbox definition and that sourced _this_ script.
#       It is used to derive a hash for defining the installation directory and to set the value of
#       the LAW_SANDBOX variable.
#   CF_VENV_BASE
#       The base directory where virtual environments will be installed. Set by the main setup.sh.
#   CF_VENV_NAME
#       The name of the virtual environment. It will be installed into $CF_VENV_BASE/$CF_VENV_NAME.
#   CF_VENV_REQUIREMENTS
#       The requirements file containing packages that are installed on top of
#       $CF_BASE/sandboxes/cf.txt.
#
# Upon venv activation, two environment variables are set in addition to those exported by the venv:
#   CF_DEV
#       Set to "1" when CF_VENV_NAME ends with "_dev", and "0" otherwise.
#   LAW_SANDBOX
#       Set to the name of the sandbox to be correctly interpreted later on by law. See
#       CF_SANDBOX_FILE above.
#
# Optional arguments:
#   1. mode
#      The setup mode. Different values are accepted:
#        - ''/install: The virtual environment is installed when not existing yet and sourced.
#        - reinstall:  The virtual environment is removed first, then reinstalled and sourced.
#        - update:     The virtual environment is removed first in case it is outdated, then
#                      reinstalled and sourced.
#      Please note that if the mode is empty ('') and the environment variable CF_SANDBOX_SETUP_MODE
#      is defined, its value is used instead.
#
#   2. versioncheck
#      When "yes", perform a version check, print a warning in case of a mismatch and set a specific
#      exit code (21). When "no", the check is skipped alltogether. When "silent", no warning is
#      printed but an exit code might be set. When "warn" (the default), a warning might be printed,
#      but the exit code remains unchanged.
#
# Note on remote jobs:
# When the CF_REMOTE_ENV variable is found to be "1" (usually set by a remote job bootstrap script),
# no mode is supported and an error is printed when it is set to a non-empty value. In any case, no
# installation will happen but the setup is reused from a pre-compiled software bundle that is
# fetched from a local or remote location and unpacked.

setup_venv() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local orig_dir="${PWD}"

    # zsh options
    if ${shell_is_zsh}; then
        emulate -L bash
        setopt globdots
    fi

    # source the main setup script to access helpers
    CF_SKIP_SETUP="1" source "${this_dir}/../setup.sh" "" || return "$?"


    #
    # get and check arguments
    #

    local mode="${1:-}"
    local versioncheck="${2:-warn}"

    # default mode
    if [ -z "${mode}" ]; then
        if [ ! -z "${CF_SANDBOX_SETUP_MODE}" ]; then
            mode="${CF_SANDBOX_SETUP_MODE}"
        else
            mode="install"
        fi
    fi

    # value checks
    if [ "${mode}" != "install" ] && [ "${mode}" != "reinstall" ] && [ "${mode}" != "update" ]; then
        >&2 echo "unknown venv setup mode '${mode}'"
        return "1"
    fi
    if [ "${CF_REMOTE_ENV}" = "1" ] && [ "${mode}" != "install" ]; then
        >&2 echo "the venv setup mode must be 'install' or empty in remote jobs, but got '${mode}'"
        return "2"
    fi
    if [ "${versioncheck}" != "yes" ] && [ "${versioncheck}" != "no" ] && [ "${versioncheck}" != "silent" ] && [ "${versioncheck}" != "warn" ]; then
        >&2 echo "unknown versioncheck setting '${versioncheck}'"
        return "3"
    fi


    #
    # check required global variables
    #

    local sandbox_file="${CF_SANDBOX_FILE}"
    unset CF_SANDBOX_FILE
    if [ -z "${sandbox_file}" ]; then
        >&2 echo "CF_SANDBOX_FILE is not set but required by ${this_file}"
        return "10"
    fi
    if [ -z "${CF_VENV_NAME}" ]; then
        >&2 echo "CF_VENV_NAME is not set but required by ${this_file}"
        return "11"
    fi
    if [ -z "${CF_VENV_REQUIREMENTS}" ]; then
        >&2 echo "CF_VENV_REQUIREMENTS is not set but required by ${this_file}"
        return "12"
    fi

    # split $CF_VENV_REQUIREMENTS into an array
    local requirement_files
    local requirement_files_contains_cf="false"
    if ${shell_is_zsh}; then
        requirement_files=(${(@s:,:)CF_VENV_REQUIREMENTS})
    else
        IFS="," read -r -a requirement_files <<< "${CF_VENV_REQUIREMENTS}"
    fi
    for f in ${requirement_files[@]}; do
        if [ ! -f "${f}" ]; then
            >&2 echo "requirement file '${f}' does not exist"
            return "13"
        fi
        if [ "${f}" = "${CF_BASE}/sandboxes/cf.txt" ]; then
            requirement_files_contains_cf="true"
        fi
    done
    local first_requirement_file="${requirement_files[0]}"


    #
    # define variables
    #

    local install_hash="$( cf_sandbox_file_hash "${sandbox_file}" )"
    local venv_name_hashed="${CF_VENV_NAME}_${install_hash}"
    local install_path="${CF_VENV_BASE}/${venv_name_hashed}"
    local install_path_repr="\$CF_VENV_BASE/${venv_name_hashed}"
    local venv_version="$( cat "${first_requirement_file}" | grep -Po "# version \K\d+.*" )"
    local pending_flag_file="${CF_VENV_BASE}/pending_${venv_name_hashed}"
    local pyv="$( python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" )"

    export CF_SANDBOX_FLAG_FILE="${install_path}/cf_flag"

    # prepend persistent path fragments to priotize packages in the outer env
    export CF_VENV_PYTHONPATH="${install_path}/lib/python${pyv}/site-packages"
    export PYTHONPATH="${CF_PERSISTENT_PYTHONPATH}:${CF_VENV_PYTHONPATH}:${PYTHONPATH}"
    export PATH="${CF_PERSISTENT_PATH}:${PATH}"


    #
    # start the setup
    #

    # the venv version must be set
    if [ -z "${venv_version}" ]; then
        >&2 echo "first requirement file ${first_requirement_file} does not contain a version line"
        return "20"
    fi

    # ensure the CF_VENV_BASE exists
    mkdir -p "${CF_VENV_BASE}"

    # possible return value
    local ret="0"

    # handle local environments
    if [ "${CF_REMOTE_ENV}" != "1" ]; then
        # optionally remove the current installation
        if [ "${mode}" = "reinstall" ]; then
            echo "removing current installation at ${install_path_repr} (mode '${mode}')"
            rm -rf "${install_path}"
        fi

        # from here onwards, files and directories could be created and in order to prevent race
        # conditions from multiple processes, guard the setup with the pending_flag_file and sleep for a
        # random amount of seconds between 0 and 10 to further reduce the chance of simultaneously
        # starting processes getting here at the same time
        if [ ! -f "${CF_SANDBOX_FLAG_FILE}" ]; then
            local sleep_counter="0"
            sleep "$( python3 -c 'import random;print(random.random() * 10)')"
            # when the file is older than 30 minutes, consider it a dangling leftover from a
            # previously failed installation attempt and delete it.
            if [ -f "${pending_flag_file}" ]; then
                local flag_file_age="$(( $( date +%s ) - $( date +%s -r "${pending_flag_file}" )))"
                [ "${flag_file_age}" -ge "1800" ] && rm -f "${pending_flag_file}"
            fi
            # start the sleep loop
            while [ -f "${pending_flag_file}" ]; do
                # wait at most 20 minutes
                sleep_counter="$(( $sleep_counter + 1 ))"
                if [ "${sleep_counter}" -ge 120 ]; then
                    >&2 echo "venv ${CF_VENV_NAME} is setup in different process, but number of sleeps exceeded"
                    return "22"
                fi
                cf_color yellow "venv ${CF_VENV_NAME} already being setup in different process, sleep ${sleep_counter} / 120"
                sleep 10
            done
        fi

        # create the pending_flag to express that the venv state might be changing
        touch "${pending_flag_file}"
        clear_pending() {
            rm -f "${pending_flag_file}"
        }

        # checks to be performed if the venv already exists
        if [ -f "${CF_SANDBOX_FLAG_FILE}" ]; then
            # get the current version
            local current_version="$( cat "${CF_SANDBOX_FLAG_FILE}" | grep -Po "version \K\d+.*" )"
            if [ -z "${current_version}" ]; then
                >&2 echo "the flag file ${CF_SANDBOX_FLAG_FILE} does not contain a valid version"
                return "23"
            fi

            if [ "${current_version}" != "${venv_version}" ]; then
                if [ "${mode}" = "update" ]; then
                    # remove the venv in case an update is requested
                    echo "removing current installation at ${install_path_repr} (mode '${mode}', installed version ${current_version}, requested version ${venv_version})"
                    rm -rf "${install_path}"

                elif [ "${versioncheck}" != "no" ]; then
                    # complain about the version mismatch
                    if [ "${versioncheck}" != "warn" ]; then
                        ret="21"
                    fi
                    if [ "${versioncheck}" != "silent" ]; then
                        >&2 echo
                        >&2 echo "WARNING: outdated venv '${venv_name_hashed}'"
                        >&2 echo "WARNING: (installed version ${current_version}, requested version ${venv_version})"
                        >&2 echo "WARNING: located at ${install_path_repr}"
                        >&2 echo "WARNING: please consider updating it by adding 'update' to the source command"
                        >&2 echo "WARNING: or by setting the environment variable 'CF_SANDBOX_SETUP_MODE=update'"
                        >&2 echo
                    fi
                fi
            fi

            # activate it
            if [ -f "${CF_SANDBOX_FLAG_FILE}" ]; then
                source "${install_path}/bin/activate" "" || return "$?"
            fi
        fi

        # install if not existing
        if [ ! -f "${CF_SANDBOX_FLAG_FILE}" ]; then
            cf_color cyan "installing venv ${CF_VENV_NAME} from ${sandbox_file} at ${install_path}"

            rm -rf "${install_path}"
            cf_create_venv "${venv_name_hashed}"
            [ "$?" != "0" ] && clear_pending && return "25"

            # activate it
            source "${install_path}/bin/activate" ""
            [ "$?" != "0" ] && clear_pending && return "26"

            # compose a list of arguments containing dependencies to install
            local install_reqs=""
            add_requirements() {
                local args
                args="${@}"
                echo "$( cf_color magenta "install" ) $( cf_color default_bright "${args}" )"
                [ ! -z "${install_reqs}" ] && install_reqs="${install_reqs} "
                install_reqs="${install_reqs}${args}"
            }

            # update packaging tools
            add_requirements pip setuptools

            # basic cf requirements
            if ! ${requirement_files_contains_cf}; then
                add_requirements -r "${CF_BASE}/sandboxes/cf.txt"
            fi

            # requirement files
            local f
            for f in ${requirement_files[@]}; do
                add_requirements -r "${f}"
            done

            # actual installation
            eval "python -m pip install -I -U --no-cache-dir ${install_reqs}"
            [ "$?" != "0" ] && clear_pending && return "27"
            echo

            # make newly installed packages relocatable
            cf_make_venv_relocatable "${venv_name_hashed}"
            [ "$?" != "0" ] && clear_pending && return "28"

            # write the version and a timestamp into the flag file
            echo "version ${venv_version}" > "${CF_SANDBOX_FLAG_FILE}"
            echo "timestamp $( date "+%s" )" >> "${CF_SANDBOX_FLAG_FILE}"
        fi

        # remove the pending_flag
        clear_pending
    fi

    # handle remote job environments
    if [ "${CF_REMOTE_ENV}" = "1" ]; then
        # in this case, the environment is inside a remote job, i.e., these variables are present:
        # CF_JOB_BASH_SANDBOX_URIS, CF_JOB_BASH_SANDBOX_PATTERNS and CF_JOB_BASH_SANDBOX_NAMES
        if [ ! -f "${CF_SANDBOX_FLAG_FILE}" ]; then
            if [ -z "${CF_WLCG_TOOLS}" ] || [ ! -f "${CF_WLCG_TOOLS}" ]; then
                >&2 echo "CF_WLCG_TOOLS (${CF_WLCG_TOOLS}) files is empty or does not exist"
                return "30"
            fi

            # fetch the bundle and unpack it
            echo "looking for bash sandbox bundle for venv ${CF_VENV_NAME}"
            local sandbox_names=( ${CF_JOB_BASH_SANDBOX_NAMES} )
            local sandbox_uris=( ${CF_JOB_BASH_SANDBOX_URIS} )
            local sandbox_patterns=( ${CF_JOB_BASH_SANDBOX_PATTERNS} )
            local found_sandbox="false"
            for (( i=0; i<${#sandbox_names[@]}; i+=1 )); do
                [ "${sandbox_names[i]}" != "${CF_VENV_NAME}" ] && continue
                echo "found bundle ${CF_VENV_NAME}, index ${i}, pattern ${sandbox_patterns[i]}, uri ${sandbox_uris[i]}"
                (
                    source "${CF_WLCG_TOOLS}" "" &&
                    mkdir -p "${install_path}" &&
                    cd "${install_path}" &&
                    law_wlcg_get_file "${sandbox_uris[i]}" "${sandbox_patterns[i]}" "bundle.tgz" &&
                    tar -xzf "bundle.tgz"
                ) || return "$?"
                found_sandbox="true"
                break
            done
            if ! ${found_sandbox}; then
                >&2 echo "bash sandbox '${CF_VENV_NAME}' not found in job configuration, stopping"
                return "31"
            fi
        fi

        # let the home variable in pyvenv.cfg point to the conda bin directory
        sed -i -r \
            "s|^(home = ).+/bin/?$|\1$CF_CONDA_BASE\/bin|" \
            "${install_path}/pyvenv.cfg"

        # activate it
        source "${install_path}/bin/activate" "" || return "$?"

        echo
    fi

    # export variables
    export CF_VENV_NAME="${CF_VENV_NAME}"
    export CF_VENV_HASH="${install_hash}"
    export CF_VENV_NAME_HASHED="${venv_name_hashed}"
    export CF_DEV="$( [[ "${CF_VENV_NAME}" == *_dev ]] && echo "1" || echo "0" )"

    # mark this as a bash sandbox for law
    export LAW_SANDBOX="bash::$( cf_sandbox_file_hash -p "${sandbox_file}" )"

    return "${ret}"
}
setup_venv "$@"
