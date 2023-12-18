#!/usr/bin/env bash

# Script that installs, removes and / or sources a CMSSW environment. Distinctions are made
# depending on whether the installation is already present, and whether the script is called as part
# of a remote (law) job (CF_REMOTE_ENV=1).
#
# In case a function or executable named "cf_cmssw_custom_install" is defined, it is invoked inside
# the src directory of the CMSSW checkout after a first "scram build" pass and followed by a second
# pass. If its refers to a file, that file is sourced. Likewise, if defined, a function or
# executable named "cf_cmssw_custom_setup" is invoked after the sandbox is activated.
#
# Six environment variables are expected to be set before this script is called:
#   CF_SANDBOX_FILE
#       The path of the file that contained the sandbox definition and that sourced _this_ script.
#       It is used to derive a hash for defining the installation directory and to set the value of
#       the LAW_SANDBOX variable.
#   CF_SCRAM_ARCH
#       The scram architecture string.
#   CF_CMSSW_VERSION
#       The desired CMSSW version to setup.
#   CF_CMSSW_BASE
#       The location where the CMSSW environment should be installed.
#   CF_CMSSW_ENV_NAME
#       The name of the environment to prevent collisions between multiple environments using the
#       same CMSSW version.
#   CF_CMSSW_FLAG
#       An incremental integer value stored in the installed CMSSW environment to detect whether it
#       needs to be updated.
#
# Arguments:
#   1. mode
#      The setup mode. Different values are accepted:
#        - ''/install: The CMSSW environment is installed when not existing yet and sourced.
#        - clear:      The CMSSW environment is removed when existing.
#        - reinstall:  The CMSSW environment is removed first, then reinstalled and sourced.
#        - update:     The CMSSW environment is removed first in case it is outdated, then
#                      reinstalled and sourced.
#      Please note that if the mode is empty ('') and the environment variable CF_SANDBOX_SETUP_MODE
#      is defined, its value is used instead.
#
# Note on remote jobs:
# When the CF_REMOTE_ENV variable is found to be "1" (usually set by a remote job bootstrap script),
# no mode is supported and no installation will happen but the desired CMSSW setup is reused from a
# pre-compiled CMSSW bundle that is fetched from a local or remote location and unpacked.

setup_cmssw() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local orig_dir="$( pwd )"

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

    # default mode
    if [ -z "${mode}" ]; then
        mode="install"
        [ ! -z "${CF_SANDBOX_SETUP_MODE}" ] && mode="${CF_SANDBOX_SETUP_MODE}"
    fi

    # force install mode for remote jobs
    [ "${CF_REMOTE_ENV}" = "1" ] && mode="install"

    # value checks
    if [ "${mode}" != "install" ] && [ "${mode}" != "clear" ] && [ "${mode}" != "reinstall" ] && [ "${mode}" != "update" ]; then
        >&2 echo "unknown CMSSW setup mode '${mode}'"
        return "1"
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
    if [ -z "${CF_SCRAM_ARCH}" ]; then
        >&2 echo "CF_SCRAM_ARCH is not set but required by ${this_file} to setup CMSSW"
        return "11"
    fi
    if [ -z "${CF_CMSSW_VERSION}" ]; then
        >&2 echo "CF_CMSSW_VERSION is not set but required by ${this_file} to setup CMSSW"
        return "12"
    fi
    if [ -z "${CF_CMSSW_BASE}" ]; then
        >&2 echo "CF_CMSSW_BASE is not set but required by ${this_file} to setup CMSSW"
        return "13"
    fi
    if [ -z "${CF_CMSSW_ENV_NAME}" ]; then
        >&2 echo "CF_CMSSW_ENV_NAME is not set but required by ${this_file} to setup CMSSW"
        return "14"
    fi
    if [ -z "${CF_CMSSW_FLAG}" ]; then
        >&2 echo "CF_CMSSW_FLAG is not set but required by ${this_file} to setup CMSSW"
        return "15"
    fi


    #
    # define variables
    #

    local install_hash="$( cf_sandbox_file_hash "${sandbox_file}" )"
    local cmssw_env_name_hashed="${CF_CMSSW_ENV_NAME}_${install_hash}"
    local install_base="${CF_CMSSW_BASE}/${cmssw_env_name_hashed}"
    local install_path="${install_base}/${CF_CMSSW_VERSION}"
    local install_path_repr="\$CF_CMSSW_BASE/${cmssw_env_name_hashed}/${CF_CMSSW_VERSION}"
    local pending_flag_file="${CF_CMSSW_BASE}/pending_${cmssw_env_name_hashed}_${CF_CMSSW_VERSION}"

    export CF_SANDBOX_FLAG_FILE="${install_path}/cf_flag"


    #
    # start the setup
    #

    # ensure CF_CMSSW_BASE exists
    mkdir -p "${CF_CMSSW_BASE}"

    if [ "${CF_REMOTE_ENV}" != "1" ]; then
        # optionally remove the current installation
        if [ "${mode}" = "clear" ] || [ "${mode}" = "reinstall" ]; then
            echo "removing current installation at ${install_path} (mode '${mode}')"
            rm -rf "${install_path}"

            # optionally stop here
            [ "${mode}" = "clear" ] && return "0"
        fi

        # in local environments, install from scratch
        if [ ! -d "${install_path}" ]; then
            # from here onwards, files and directories could be created and in order to prevent race
            # conditions from multiple processes, guard the setup with the pending_flag_file and
            # sleep for a random amount of seconds between 0 and 10 to further reduce the chance of
            # simultaneously starting processes getting here at the same time
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
                    >&2 echo "${CF_CMSSW_VERSION} is setup in different process, but number of sleeps exceeded"
                    return "20"
                fi
                cf_color yellow "${CF_CMSSW_VERSION} already being setup in different process, sleep ${sleep_counter} / 120"
                sleep 10
            done
        fi

        # create the pending_flag to express that the venv state might be changing
        touch "${pending_flag_file}"
        clear_pending() {
            rm -f "${pending_flag_file}"
        }

        # checks to be performed if the venv already exists
        if [ -d "${install_path}" ]; then
            # get the current version
            local current_version="$( cat "${CF_SANDBOX_FLAG_FILE}" | grep -Po "version \K\d+.*" )"
            if [ -z "${current_version}" ]; then
                >&2 echo "the flag file ${CF_SANDBOX_FLAG_FILE} does not contain a valid version"
                clear_pending
                return "21"
            fi

            if [ "${current_version}" != "${CF_CMSSW_FLAG}" ]; then
                if [ "${mode}" = "update" ]; then
                    # remove the venv in case an update is requested
                    echo "removing current installation at ${install_path_repr}"
                    echo "(mode '${mode}', installed version ${current_version}, requested version ${CF_CMSSW_FLAG})"
                    rm -rf "${install_path}"
                else
                    >&2 echo
                    >&2 echo "WARNING: outdated CMSSW environment '${cmssw_env_name_hashed}'"
                    >&2 echo "WARNING: (installed version ${current_version}, requested version ${CF_CMSSW_FLAG})"
                    >&2 echo "WARNING: located at ${install_path_repr}"
                    >&2 echo "WARNING: please consider updating it by adding 'update' to the source command"
                    >&2 echo "WARNING: or by setting the environment variable 'CF_SANDBOX_SETUP_MODE=update'"
                    >&2 echo
                fi
            fi
        fi

        # install when missing
        if [ ! -d "${install_path}" ]; then
            echo
            cf_color cyan "installing ${CF_CMSSW_VERSION} on ${CF_SCRAM_ARCH} in ${install_base}"

            # first install pass
            (
                mkdir -p "${install_base}"
                cd "${install_base}"
                export SCRAM_ARCH="${CF_SCRAM_ARCH}"
                source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" &&
                scramv1 project CMSSW "${CF_CMSSW_VERSION}" &&
                cd "${CF_CMSSW_VERSION}/src" &&
                eval "$( scramv1 runtime -sh )" &&
                scram b
            )
            local ret="$?"
            [ "${ret}" != "0" ] && clear_pending && return "${ret}"

            # custom install hook and second install pass
            (
                cd "${install_path}/src"
                source "/cvmfs/cms.cern.ch/cmsset_default.sh" ""
                eval "$( scramv1 runtime -sh )"

                if command -v cf_cmssw_custom_install &> /dev/null; then
                    echo -e "\nrunning cf_cmssw_custom_install"
                    cf_cmssw_custom_install &&
                    cd "${install_path}/src" &&
                    scram b
                elif [ ! -z "${cf_cmssw_custom_install}" ] && [ -f "${cf_cmssw_custom_install}" ]; then
                    echo -e "\nsourcing cf_cmssw_custom_install file"
                    source "${cf_cmssw_custom_install}" "" &&
                    cd "${install_path}/src" &&
                    scram b
                fi
            )
            ret="$?"
            [ "${ret}" != "0" ] && clear_pending && return "${ret}"

            # write the flag into a file
            echo "version ${CF_CMSSW_FLAG}" > "${CF_SANDBOX_FLAG_FILE}"
            clear_pending
        fi

        # remove the pending_flag
        clear_pending
    fi

    # handle remote job environments
    if [ "${CF_REMOTE_ENV}" = "1" ]; then
        # fetch, unpack and setup the bundle
        if [ ! -d "${install_path}" ]; then
            if [ -z "${CF_WLCG_TOOLS}" ] || [ ! -f "${CF_WLCG_TOOLS}" ]; then
                >&2 echo "CF_WLCG_TOOLS (${CF_WLCG_TOOLS}) files is empty or does not exist"
                return "30"
            fi

            # fetch the bundle and unpack it
            echo "looking for cmssw sandbox bundle for${CF_CMSSW_ENV_NAME}"
            local sandbox_names=( ${CF_JOB_CMSSW_SANDBOX_NAMES} )
            local sandbox_uris=( ${CF_JOB_CMSSW_SANDBOX_URIS} )
            local sandbox_patterns=( ${CF_JOB_CMSSW_SANDBOX_PATTERNS} )
            local found_sandbox="false"
            for (( i=0; i<${#sandbox_names[@]}; i+=1 )); do
                if [ "${sandbox_names[i]}" = "${CF_CMSSW_ENV_NAME}" ]; then
                    echo "found bundle ${CF_CMSSW_ENV_NAME}, index ${i}, pattern ${sandbox_patterns[i]}, uri ${sandbox_uris[i]}"
                    (
                        source "${CF_WLCG_TOOLS}" "" &&
                        mkdir -p "${install_base}" &&
                        cd "${install_base}" &&
                        law_wlcg_get_file "${sandbox_uris[i]}" "${sandbox_patterns[i]}" "cmssw.tgz"
                    ) || return "$?"
                    found_sandbox="true"
                    break
                fi
            done
            if ! ${found_sandbox}; then
                >&2 echo "cmssw sandbox ${CF_CMSSW_ENV_NAME} not found in job configuration, stopping"
                return "22"
            fi

            # create a new cmssw checkout, unpack the bundle on top and rebuild python symlinks
            (
                echo "unpacking bundle to ${install_path}" &&
                cd "${install_base}" &&
                export SCRAM_ARCH="${CF_SCRAM_ARCH}" &&
                export PATH="${LAW_CRAB_ORIGINAL_PATH:-${PATH}}" &&
                export LD_LIBRARY_PATH="${LAW_CRAB_ORIGINAL_LD_LIBRARY_PATH:-${LD_LIBRARY_PATH}}" &&
                source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" &&
                scramv1 project CMSSW "${CF_CMSSW_VERSION}" &&
                cd "${CF_CMSSW_VERSION}" &&
                tar -xzf "../cmssw.tgz" &&
                rm "../cmssw.tgz" &&
                cd "src" &&
                eval "$( scramv1 runtime -sh )" &&
                scram b python
            ) || return "$?"

            # write the flag into a file
            echo "version ${CF_CMSSW_FLAG}" > "${CF_SANDBOX_FLAG_FILE}"
        fi
    fi

    # at this point, the src path must exist
    if [ ! -d "${install_path}/src" ]; then
        >&2 echo "src directory not found in CMSSW installation at ${install_path}"
        return "30"
    fi

    # source it
    source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" || return "$?"
    export SCRAM_ARCH="${CF_SCRAM_ARCH}"
    export CMSSW_VERSION="${CF_CMSSW_VERSION}"
    cd "${install_path}/src"
    eval "$( scramv1 runtime -sh )" || return "$?"

    # custom setup
    if command -v cf_cmssw_custom_setup &> /dev/null; then
        cf_cmssw_custom_setup || return "$?"
    elif [ ! -z "${cf_cmssw_custom_setup}" ] && [ -f "${cf_cmssw_custom_setup}" ]; then
        source "${cf_cmssw_custom_setup}" "" || return "$?"
    fi

    cd "${orig_dir}"

    # prepend persistent path fragments again to ensure priority for local packages and
    # remove the conda based python fragments since there are too many overlaps between packages
    export PYTHONPATH="${CF_PERSISTENT_PATH}:$( echo ${PYTHONPATH} | sed "s|${CF_CONDA_PYTHONPATH}||g" )"
    export PATH="${CF_PERSISTENT_PATH}:${PATH}"

    # mark this as a bash sandbox for law
    export LAW_SANDBOX="bash::$( cf_sandbox_file_hash -p "${sandbox_file}" )"

    return "0"
}
setup_cmssw "$@"
