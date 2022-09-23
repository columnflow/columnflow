#!/usr/bin/env bash

# Script that installs, removes and / or sources a CMSSW environment. Distinctions are made
# depending on whether the installation is already present, and whether the script is called as part
# of a remote (law) job (CF_REMOTE_JOB=1).
#
# Five environment variables are expected to be set before this script is called:
#   - CF_SCRAM_ARCH:
#       The scram architecture string.
#   - CF_CMSSW_VERSION:
#       The desired CMSSW version to setup.
#   - CF_CMSSW_BASE:
#       The location where the CMSSW environment should be installed.
#   - CF_CMSSW_ENV_NAME:
#       The name of the environment to prevent collisions between multiple environments using the
#       same CMSSW version.
#   - CF_CMSSW_FLAG:
#       An incremental integer value stored in the installed CMSSW environment to detect whether it
#       needs to be updated.
#
# Arguments:
# 1. mode: The setup mode. Different values are accepted:
#   - '' (default): The CMSSW environment is installed when not existing yet and sourced.
#   - clear:        The CMSSW environment is removed when existing.
#   - reinstall:    The CMSSW environment is removed first, then reinstalled and sourced.
#   - install_only: The CMSSW environment is installed when not existing yet but not sourced.
#
# Note on remote jobs:
# When the CF_REMOTE_JOB variable is found to be "1" (usually set by a remote job bootstrap script),
# no mode is supported and an error is printed when it is set to a non-empty value. In any case, no
# installation will happen but the desired CMSSW setup is reused from a pre-compiled CMSSW bundle
# that is fetched from a local or remote location and unpacked.

setup_cmssw() {
    local shell_is_zsh=$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local orig_dir="$( pwd )"


    #
    # get and check arguments
    #

    local mode="${1:-}"
    if [ ! -z "${mode}" ] && [ "${mode}" != "clear" ] && [ "${mode}" != "reinstall" ] && [ "${mode}" != "install_only" ]; then
        >&2 echo "unknown CMSSW source mode '${mode}'"
        return "1"
    fi
    if [ "${CF_REMOTE_JOB}" = "1" ] && [ ! -z "${mode}" ]; then
        >&2 echo "the CMSSW source mode must be empty in remote jobs, but got '${mode}'"
        return "2"
    fi


    #
    # check required global variables
    #

    if [ -z "${CF_SCRAM_ARCH}" ]; then
        >&2 echo "CF_SCRAM_ARCH is not set but required by ${this_file} to setup CMSSW"
        return "3"
    fi
    if [ -z "${CF_CMSSW_VERSION}" ]; then
        >&2 echo "CF_CMSSW_VERSION is not set but required by ${this_file} to setup CMSSW"
        return "4"
    fi
    if [ -z "${CF_CMSSW_BASE}" ]; then
        >&2 echo "CF_CMSSW_BASE is not set but required by ${this_file} to setup CMSSW"
        return "5"
    fi
    if [ -z "${CF_CMSSW_ENV_NAME}" ]; then
        >&2 echo "CF_CMSSW_ENV_NAME is not set but required by ${this_file} to setup CMSSW"
        return "6"
    fi
    if [ -z "${CF_CMSSW_FLAG}" ]; then
        >&2 echo "CF_CMSSW_FLAG is not set but required by ${this_file} to setup CMSSW"
        return "7"
    fi


    #
    # store variables of the encapsulating venv
    #

    local pyv="$( python3 -c "import sys; print('{0.major}.{0.minor}'.format(sys.version_info))" )"
    local venv_site_packages="${CF_VENV_BASE}/${CF_VENV_NAME}/lib/python${pyv}/site-packages"


    #
    # start the setup
    #

    [ -z "${GFAL_PLUGIN_DIR_ORIG}" ] && export GFAL_PLUGIN_DIR_ORIG="${GFAL_PLUGIN_DIR}"
    local install_base="${CF_CMSSW_BASE}/${CF_CMSSW_ENV_NAME}"
    local install_path="${install_base}/${CF_CMSSW_VERSION}"
    local pending_flag_file="${CF_CMSSW_BASE}/pending_${CF_CMSSW_ENV_NAME}_${CF_CMSSW_VERSION}"
    export CF_SANDBOX_FLAG_FILE="${install_path}/cf_flag"

    # ensure CF_CMSSW_BASE exists
    mkdir -p "${CF_CMSSW_BASE}"

    # remove the current installation
    if [ "${mode}" = "clear" ] || [ "${mode}" = "reinstall" ]; then
        echo "removing current installation at $install_path (mode '${mode}')"
        rm -rf "${install_path}"

        # optionally stop here
        [ "${mode}" = "clear" ] && return "0"
    fi

    if [ "${CF_REMOTE_JOB}" == "1" ]; then
        # in remote jobs, fetch and setup the bundle
        if [ ! -d "${install_path}" ]; then
            # determine the bundle to unpack
            local bundle="${CF_SOFTWARE}/cmssw_sandboxes/${CF_CMSSW_ENV_NAME}.tgz"
            if [ ! -f "$bundle" ]; then
                >&2 echo "prefetched bundle expected at ${bundle} not does not exist"
                return "8"
            fi

            # create a new cmssw checkout, unpack the bundle on top and rebuild python symlinks
            (
                echo "unpacking bundle ${bundle} to ${install_path}"
                mkdir -p "${install_base}" || return "$?"
                cd "${install_base}"
                source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" || return "$?"
                export SCRAM_ARCH="${CF_SCRAM_ARCH}"
                scramv1 project CMSSW "${CF_CMSSW_VERSION}" || return "$?"
                cd "${CF_CMSSW_VERSION}"
                cp "${bundle}" .
                tar -xzf "$( basename "${bundle}" )" || return "$?"
                cd "src" || return "$?"
                eval "$( scramv1 runtime -sh )" || return "$?"
                scram b python || return "$?"
            ) || return "$?"

            # write the flag into a file
            echo "version ${CF_CMSSW_FLAG}" > "${CF_SANDBOX_FLAG_FILE}"
        fi
    else
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
                # wait at most 10 minutes
                sleep_counter="$(( $sleep_counter + 1 ))"
                if [ "${sleep_counter}" -ge 120 ]; then
                    >&2 echo "cmssw ${CF_CMSSW_VERSION} is setup in different process, but number of sleeps exceeded"
                    return "8"
                fi
                echo -e "\x1b[0;49;36mcmssw ${CF_CMSSW_VERSION} already being setup in different process, sleep ${sleep_counter} / 120\x1b[0m"
                sleep 5
            done
        fi

        if [ ! -d "${install_path}" ]; then
            local ret
            touch "${pending_flag_file}"
            echo "installing ${CF_CMSSW_VERSION} in ${install_base}"

            (
                mkdir -p "${install_base}" || ( ret="$?" && rm -f "${pending_flag_file}" && return "${ret}" )
                cd "${install_base}"
                source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" || ( ret="$?" && rm -f "${pending_flag_file}" && return "${ret}" )
                export SCRAM_ARCH="${CF_SCRAM_ARCH}"
                scramv1 project CMSSW "${CF_CMSSW_VERSION}" || ( ret="$?" && rm -f "${pending_flag_file}" && return "${ret}" )
                cd "${CF_CMSSW_VERSION}/src"
                eval "$( scramv1 runtime -sh )" || ( ret="$?" && rm -f "${pending_flag_file}" && return "${ret}" )
                scram b || ( ret="$?" && rm -f "${pending_flag_file}" && return "${ret}" )

                # create symlinks to gfal plugins and remove the http plugin which is
                # not compatible with cmssw
                (
                    mkdir -p "${install_path}/lib/gfal2"
                    cd "${install_path}/lib/gfal2"
                    ln -s ${GFAL_PLUGIN_DIR_ORIG}/*.so .
                    rm -r libgfal_plugin_http.so
                )

                # write the flag into a file
                echo "version ${CF_CMSSW_FLAG}" > "${CF_SANDBOX_FLAG_FILE}"
                rm -f "${pending_flag_file}"
            ) || ( ret="$?" && rm -f "${pending_flag_file}" && return "${ret}" )
        fi
    fi

    # at this point, the src path must exist
    if [ ! -d "${install_path}/src" ]; then
        >&2 echo "src directory not found in CMSSW installation at ${install_path}"
        return "9"
    fi

    # check the flag and show a warning when there was an update
    if [ "$( cat "${CF_SANDBOX_FLAG_FILE}" | grep -Po "version \K\d+.*" )" != "${CF_CMSSW_FLAG}" ]; then
        >&2 echo ""
        >&2 echo "WARNING: the CMSSW software environment ${CF_CMSSW_ENV_NAME} seems to be outdated"
        >&2 echo "WARNING: please consider removing (mode 'clear') or updating it (mode 'reinstall')"
        >&2 echo ""
    fi

    # optionally stop here
    [ "${mode}" = "install_only" ] && return "0"

    # source it
    source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" || return "$?"
    export SCRAM_ARCH="${CF_SCRAM_ARCH}"
    export CMSSW_VERSION="${CF_CMSSW_VERSION}"
    export GFAL_PLUGIN_DIR="${install_path}/lib/gfal2"
    cd "${install_path}/src"
    eval "$( scramv1 runtime -sh )"
    cd "${orig_dir}"

    # prepend persistent path fragments again for ensure priority for local packages
    export PATH="${CF_PERSISTENT_PATH}:${PATH}"
    export PYTHONPATH="${CF_PERSISTENT_PYTHONPATH}:${PYTHONPATH}"

    # prepend the site-packages of the encapsulating venv for priotity over cmssw-shipped packages
    # note: this could potentially lead to version mismatches so in case this becomes in issue,
    # this injection needs refactoring
    export PYTHONPATH="${venv_site_packages}:${PYTHONPATH}"

    # mark this as a bash sandbox for law
    export LAW_SANDBOX="bash::\$CF_BASE/sandboxes/${CF_CMSSW_ENV_NAME}.sh"
}
setup_cmssw "$@"
