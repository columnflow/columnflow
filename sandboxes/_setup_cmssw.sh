#!/usr/bin/env bash

# Script that installs, removes and / or sources a CMSSW environment. Distinctions are made
# depending on whether the installation is already present, and whether the script is called as part
# of a remote (law) job (AP_REMOTE_JOB=1).
#
# Five environment variables are expected to be set before this script is called:
#   - AP_SCRAM_ARCH:
#       The scram architecture string.
#   - AP_CMSSW_VERSION:
#       The desired CMSSW version to setup.
#   - AP_CMSSW_BASE:
#       The location where the CMSSW environment should be installed.
#   - AP_CMSSW_ENV_NAME:
#       The name of the environment to prevent collisions between multiple environments using the
#       same CMSSW version.
#   - AP_CMSSW_FLAG:
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
# When the AP_REMOTE_JOB variable is found to be "1" (usually set by a remote job bootstrap script),
# no mode is supported and an error is printed when it is set to a non-empty value. In any case, no
# installation will happen but the desired CMSSW setup is reused from a pre-compiled CMSSW bundle
# that is fetched from a local or remote location and unpacked.

action() {
    local this_file="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "$this_file" )" && pwd )"
    local orig_dir="$( pwd )"


    #
    # get and check arguments
    #

    local mode="${1:-}"
    if [ "$AP_REMOTE_JOB" = "1" ]; then
        if [ ! -z "$mode" ]; then
            >&2 echo "the CMSSW source mode must be empty in remote jobs, but got '$mode'"
            return "1"
        fi
    elif [ ! -z "$mode" ] && [ "$mode" != "clear" ] && [ "$mode" != "reinstall" ] && [ "$mode" != "install_only" ]; then
        >&2 echo "unknown CMSSW source mode '$mode'"
        return "2"
    fi


    #
    # check required global variables
    #

    if [ -z "$AP_SCRAM_ARCH" ]; then
        >&2 echo "AP_SCRAM_ARCH is not set but required by $this_file to setup CMSSW"
        return "3"
    fi
    if [ -z "$AP_CMSSW_VERSION" ]; then
        >&2 echo "AP_CMSSW_VERSION is not set but required by $this_file to setup CMSSW"
        return "4"
    fi
    if [ -z "$AP_CMSSW_BASE" ]; then
        >&2 echo "AP_CMSSW_BASE is not set but required by $this_file to setup CMSSW"
        return "5"
    fi
    if [ -z "$AP_CMSSW_ENV_NAME" ]; then
        >&2 echo "AP_CMSSW_ENV_NAME is not set but required by $this_file to setup CMSSW"
        return "6"
    fi
    if [ -z "$AP_CMSSW_FLAG" ]; then
        >&2 echo "AP_CMSSW_FLAG is not set but required by $this_file to setup CMSSW"
        return "7"
    fi


    #
    # start the setup
    #

    local install_base="$AP_CMSSW_BASE/$AP_CMSSW_ENV_NAME"
    local install_path="$install_base/$AP_CMSSW_VERSION"
    local flag_file="$install_path/ap_flag"

    # remove the current installation
    if [ "$mode" = "clear" ] || [ "$mode" = "reinstall" ]; then
        echo "removing current installation at $install_path (mode '$mode')"
        rm -rf "$install_path"

        # optionally stop here
        [ "$mode" = "clear" ] && return "0"
    fi

    # install or fetch when not existing
    if [ ! -d "$install_path" ]; then
        if [ "$AP_REMOTE_JOB" != "1" ]; then
            (
                echo "installing $AP_CMSSW_VERSION in $install_base"
                mkdir -p "$install_base" || return "$?"
                cd "$install_base"
                source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" || return "$?"
                export SCRAM_ARCH="$AP_SCRAM_ARCH"
                scramv1 project CMSSW "$AP_CMSSW_VERSION" || return "$?"
                cd "$AP_CMSSW_VERSION/src"
                eval "$( scramv1 runtime -sh )" || return "$?"
                scram b || return "$?"

                # create symlinks to gfal plugins and remove the http plugin which is
                # not compatible with cmssw
                (
                    mkdir -p "$install_path/lib/gfal2"
                    cd "$install_path/lib/gfal2"
                    ln -s $GFAL_PLUGIN_DIR/*.so .
                    rm -r libgfal_plugin_http.so
                )

                # write the flag into a file
                echo "version $AP_CMSSW_FLAG" > "$flag_file"
            ) || return "$?"

        else
            # determine the bundle to unpack
            local bundle="$AP_SOFTWARE/cmssw_sandboxes/$AP_CMSSW_ENV_NAME.tgz"
            if [ ! -f "$bundle" ]; then
                >&2 echo "prefetched bundle expected at $bundle not does not exist"
                return "8"
            fi

            # create a new cmssw checkout, unpack the bundle on top and rebuild python symlinks
            (
                echo "unpacking bundle $bundle to $install_path"
                mkdir -p "$install_base" || return "$?"
                cd "$install_base"
                source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" || return "$?"
                export SCRAM_ARCH="$AP_SCRAM_ARCH"
                scramv1 project CMSSW "$AP_CMSSW_VERSION" || return "$?"
                cd "$AP_CMSSW_VERSION"
                cp "$bundle" .
                tar -xzf "$( basename "$bundle" )" || return "$?"
                cd "src" || return "$?"
                eval "$( scramv1 runtime -sh )" || return "$?"
                scram b python || return "$?"
            ) || return "$?"

            # write the flag into a file
            echo "version $AP_CMSSW_FLAG" > "$flag_file"
        fi
    fi

    # at this point, the src path must exist
    if [ ! -d "$install_path/src" ]; then
        >&2 echo "src directory not found in CMSSW installation at $install_path"
        return "7"
    fi

    # check the flag and show a warning when there was an update
    if [ "$( cat "$flag_file" | grep -Po "version \K\d+.*" )" != "$AP_CMSSW_FLAG" ]; then
        >&2 echo ""
        >&2 echo "WARNING: the CMSSW software environment $AP_CMSSW_ENV_NAME seems to be outdated"
        >&2 echo "WARNING: please consider removing (mode 'clear') or updating it (mode 'reinstall')"
        >&2 echo ""
    fi

    # optionally stop here
    [ "$mode" = "install_only" ] && return "0"

    # source it
    source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" || return "$?"
    export SCRAM_ARCH="$AP_SCRAM_ARCH"
    export CMSSW_VERSION="$AP_CMSSW_VERSION"
    export GFAL_PLUGIN_DIR="$install_path/lib/gfal2"
    cd "$install_path/src"
    eval "$( scramv1 runtime -sh )"
    cd "$orig_dir"

    # mark this as a bash sandbox for law
    export LAW_SANDBOX="bash::\$AP_BASE/sandboxes/$AP_CMSSW_ENV_NAME.sh"
}
action "$@"
