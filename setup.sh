#!/usr/bin/env bash

setup() {
    #
    # prepare local variables
    #

    local this_file="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "$this_file" )" && pwd )"
    local orig="$PWD"
    local setup_name="${1:-default}"
    local setup_is_default="false"
    [ "$setup_name" = "default" ] && setup_is_default="true"
    local shell_is_bash="false"
    [ ! -z "$BASH_VERSION" ] && shell_is_bash="true"


    #
    # global variables
    # (AP = Analysis Playground)
    #

    export AP_BASE="$this_dir"
    interactive_setup "$setup_name" || return "$?"
    export AP_SETUP_NAME="$setup_name"
    export AP_STORE_REPO="$AP_BASE/data/store"
    export AP_ORIG_PATH="$PATH"
    export AP_ORIG_PYTHONPATH="$PYTHONPATH"
    export AP_ORIG_PYTHON3PATH="$PYTHON3PATH"
    export AP_ORIG_LD_LIBRARY_PATH="$LD_LIBRARY_PATH"

    # lang defaults
    [ -z "$LANGUAGE" ] && export LANGUAGE="en_US.UTF-8"
    [ -z "$LANG" ] && export LANG="en_US.UTF-8"
    [ -z "$LC_ALL" ] && export LC_ALL="en_US.UTF-8"

    # proxy
    [ -z "$X509_USER_PROXY" ] && export X509_USER_PROXY="/tmp/x509up_u$( id -u )"


    #
    # helper functions
    #

    # helper to create or check a voms proxy
    ap_voms_proxy() {
        local mode="${1:-init}"
        local voms="${AP_VOMS:-cms}"
        if [ "$mode" = "check" ]; then
            voms-proxy-info
        else
            voms-proxy-init -voms "$AP_VOMS" --valid "196:00" --out "$X509_USER_PROXY"
        fi
    }
    $shell_is_bash && export -f ap_voms_proxy


    #
    # minimal local software setup
    #

    # use the latest centos7 ui from the grid setup on cvmfs
    [ -z "$AP_LCG_SETUP" ] && export AP_LCG_SETUP="/cvmfs/grid.cern.ch/centos7-ui-200122/etc/profile.d/setup-c7-ui-python3-example.sh"
    if [ ! -f "$AP_LCG_SETUP" ]; then
        2>&1 echo "LCG seutp file $AP_LCG_SETUP not existing"
        return "1"
    fi
    source "$AP_LCG_SETUP" ""

    # update paths and flags
    local pyv="$( python3 -c "import sys; print('{0.major}.{0.minor}'.format(sys.version_info))" )"
    export PATH="$AP_BASE/bin:$AP_BASE/ap/scripts:$AP_BASE/modules/law/bin:$AP_SOFTWARE/bin:$PATH"
    export PYTHONPATH="$AP_BASE/modules/law:$AP_BASE/modules/order:$PYTHONPATH"
    export PYTHONPATH="$AP_BASE:$PYTHONPATH"
    export PYTHONWARNINGS="ignore"
    export GLOBUS_THREAD_MODEL="none"
    ulimit -s unlimited

    # local python stack
    export AP_VENV_PATH="${AP_SOFTWARE}/venvs/ap_${AP_SETUP_NAME}"
    local sw_version="$( cat "${AP_BASE}/requirements.txt" | grep -Po "# version \K\d+.*" )"
    local flag_file_sw="${AP_VENV_PATH}/.sw_good"
    [ "$AP_REINSTALL_SOFTWARE" = "1" ] && rm -f "$flag_file_sw"
    if [ ! -f "$flag_file_sw" ]; then
        echo "installing software stack at ${AP_VENV_PATH}"
        rm -rf "${AP_VENV_PATH}"

        # setup a python virtual environment
        python3 -m venv "${AP_VENV_PATH}"

        # activate the virtual environment
        source "${AP_VENV_PATH}/bin/activate" ""

        # install software stack as defined in requirements.txt first
        python3 -m pip install -r "$AP_BASE/requirements.txt"

        date "+%s" > "$flag_file_sw"
        echo "version $sw_version" >> "$flag_file_sw"
    else
        source "${AP_VENV_PATH}/bin/activate" ""
    fi
    export AP_SOFTWARE_FLAG_FILES="$flag_file_sw"

    # check the version in the sw flag file and show a warning when there was an update
    if [ "$( cat "$flag_file_sw" | grep -Po "version \K\d+.*" )" != "$sw_version" ]; then
        2>&1 echo ""
        2>&1 echo "WARNING: your local software stack is not up to date, please consider updating it in a new shell with"
        2>&1 echo "         > AP_REINSTALL_SOFTWARE=1 source setup.sh $( $setup_is_default || echo "$setup_name" )"
        2>&1 echo ""
    fi


    #
    # initialze submodules
    #

    if [ -d "$AP_BASE/.git" ]; then
        for m in law order; do
            local mpath="$AP_BASE/modules/$m"
            # initialize the submodule when the directory is empty
            if [ "$( ls -1q "$mpath" | wc -l )" = "0" ]; then
                git submodule update --init --recursive "$mpath"
            else
                # update when not on a working branch and there are no changes
                local detached_head="$( ( cd "$mpath"; git symbolic-ref -q HEAD &> /dev/null ) && echo true || echo false )"
                local changed_files="$( cd "$mpath"; git status --porcelain=v1 2> /dev/null | wc -l )"
                if ! $detached_head && [ "$changed_files" = "0" ]; then
                    git submodule update --init --recursive "$mpath"
                fi
            fi
        done
    fi


    #
    # law setup
    #

    export LAW_HOME="$AP_BASE/.law"
    export LAW_CONFIG_FILE="$AP_BASE/law.cfg"

    if which law &> /dev/null; then
        # source law's bash completion scipt
        source "$( law completion )" ""

        # silently index
        law index -q
    fi
}

interactive_setup() {
    local setup_name="${1:-default}"
    local env_file="${2:-$AP_BASE/.setups/$setup_name.sh}"
    local env_file_tmp="$env_file.tmp"

    # check if the setup is the default one
    local setup_is_default="false"
    [ "$setup_name" = "default" ] && setup_is_default="true"

    # when the setup already exists and it's not the default one,
    # source the corresponding env file and stop
    if ! $setup_is_default; then
        if [ -f "$env_file" ]; then
            echo -e "using variables for setup '\x1b[0;49;35m$setup_name\x1b[0m' from $env_file"
            source "$env_file" ""
            return "0"
        else
            echo -e "no setup file $env_file found for setup '\x1b[0;49;35m$setup_name\x1b[0m'"
        fi
    fi

    export_and_save() {
        local varname="$1"
        local value="$2"

        export $varname="$( eval "echo $value" )"
        ! $setup_is_default && echo "export $varname=\"$value\"" >> "$env_file_tmp"
    }

    query() {
        local varname="$1"
        local text="$2"
        local default="$3"
        local default_text="${4:-$default}"
        local default_raw="$default"

        # when the setup is the default one, use the default value when the env variable is empty,
        # otherwise, query interactively
        local value="$default"
        if $setup_is_default; then
            [ ! -z "${!varname}" ] && value="${!varname}"
        else
            printf "$text (\x1b[1;49;39m$varname\x1b[0m, default \x1b[1;49;39m$default_text\x1b[0m):  "
            read query_response
            [ "X$query_response" = "X" ] && query_response="$default"

            # repeat for boolean flags that were not entered correctly
            while true; do
                ( [ "$default" != "True" ] && [ "$default" != "False" ] ) && break
                ( [ "$query_response" = "True" ] || [ "$query_response" = "False" ] ) && break
                printf "please enter either '\x1b[1;49;39mTrue\x1b[0m' or '\x1b[1;49;39mFalse\x1b[0m':  " query_response
                read query_response
                [ "X$query_response" = "X" ] && query_response="$default"
            done
            value="$query_response"

            # strip " and ' on both sides
            value=${value%\"}
            value=${value%\'}
            value=${value#\"}
            value=${value#\'}
        fi

        export_and_save "$varname" "$value"
    }

    # prepare the tmp env file
    if ! $setup_is_default; then
        rm -rf "$env_file_tmp"
        mkdir -p "$( dirname "$env_file_tmp" )"

        echo -e "Start querying variables for setup '\x1b[0;49;35m$setup_name\x1b[0m', press enter to accept default values\n"
    fi

    # start querying for variables
    query AP_DESY_USER "DESY username" "$( whoami )"
    export_and_save AP_DESY_USER_FIRSTCHAR "\${AP_DESY_USER:0:1}"
    query AP_CERN_USER "CERN username" "$( whoami )"
    export_and_save AP_CERN_USER_FIRSTCHAR "\${AP_CERN_USER:0:1}"
    query AP_DATA "Local data directory" "\$AP_BASE/data" "./data"
    query AP_STORE_NAME "Relative path used in store paths (see next queries)" "ap_store"
    query AP_STORE_LOCAL "Default local output store" "\$AP_DATA/\$AP_STORE_NAME"
    query AP_WLCG_CACHE_ROOT "Local directory for caching remote files" "" "''"
    export_and_save AP_WLCG_USE_CACHE "$( [ -z "$AP_WLCG_CACHE_ROOT" ] && echo false || echo true )"
    query AP_SOFTWARE "Local directory for installing software" "\$AP_DATA/software"
    query AP_CMSSW_BASE "Local directory for installing CMSSW" "\$AP_DATA/cmssw"
    query AP_JOB_BASE "Local directory for storing job files" "\$AP_DATA/jobs"
    query AP_LOCAL_SCHEDULER "Use a local scheduler for law tasks" "True"
    if [ "$AP_LOCAL_SCHEDULER" != "True" ]; then
        query AP_SCHEDULER_HOST "Address of a central scheduler for law tasks" "naf-cms15.desy.de"
        query AP_SCHEDULER_PORT "Port of a central scheduler for law tasks" "8082"
    else
        export_and_save AP_SCHEDULER_HOST "naf-cms15.desy.de"
        export_and_save AP_SCHEDULER_PORT "8082"
    fi
    query AP_VOMS "Virtual-organization" "cms:/cms/dcms"

    # move the env file to the correct location for later use
    if ! $setup_is_default; then
        mv "$env_file_tmp" "$env_file"
        echo -e "\nvariables written to $env_file"
    fi
}

action() {
    if setup "$@"; then
        echo -e "\x1b[0;49;35manalysis playground successfully setup\x1b[0m"
        return "0"
    else
        local code="$?"
        echo -e "\x1b[0;49;31msetup failed with code $code\x1b[0m"
        return "$code"
    fi
}
action "$@"
