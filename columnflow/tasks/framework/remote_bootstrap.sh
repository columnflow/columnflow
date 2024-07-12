#!/usr/bin/env bash

# Bootstrap file that is executed in remote jobs submitted by law to set up the environment.
# So-called render variables, denoted by "{{name}}", are replaced with variables configured in the
# remote workflow tasks, e.g. in HTCondorWorkflow.htcondor_job_config() upon job submission.

# Bootstrap function for standalone htcondor jobs.
bootstrap_htcondor_standalone() {
    # set env variables
    export CF_ON_HTCONDOR="1"
    export CF_REMOTE_ENV="1"
    export CF_CERN_USER="{{cf_cern_user}}"
    export CF_REPO_BASE="${LAW_JOB_HOME}/repo"
    export CF_DATA="${LAW_JOB_HOME}/cf_data"
    export CF_SOFTWARE_BASE="{{cf_software_base}}"
    export CF_STORE_NAME="{{cf_store_name}}"
    export CF_STORE_LOCAL="{{cf_store_local}}"
    export CF_LOCAL_SCHEDULER="{{cf_local_scheduler}}"
    export CF_WLCG_CACHE_ROOT="${LAW_JOB_HOME}/cf_wlcg_cache"
    export CF_WLCG_TOOLS="{{wlcg_tools}}"
    export LAW_CONFIG_FILE="{{law_config_file}}"
    if [ ! -z "{{vomsproxy_file}}" ]; then
        export X509_USER_PROXY="${PWD}/{{vomsproxy_file}}"
        # also move it to the /tmp/x509up_u<uid> location as some packages expect
        # the file to be at this path and do not respect the X509_USER_PROXY env var
        local tmp_x509="/tmp/x509up_u$( id -u )"
        [ ! -f "${tmp_x509}" ] && cp "${X509_USER_PROXY}" "${tmp_x509}"
    fi
    local sharing_software="$( [ -z "{{cf_software_base}}" ] && echo "false" || echo "true" )"
    local lcg_setup="{{cf_remote_lcg_setup}}"
    lcg_setup="${lcg_setup:-/cvmfs/grid.cern.ch/alma9-ui-test/etc/profile.d/setup-alma9-test.sh}"

    # temporary fix for missing voms/x509 variables in the lcg setup
    export X509_CERT_DIR="/cvmfs/grid.cern.ch/etc/grid-security/certificates"
    export X509_VOMS_DIR="/cvmfs/grid.cern.ch/etc/grid-security/vomsdir"
    export X509_VOMSES="/cvmfs/grid.cern.ch/etc/grid-security/vomses"
    export VOMS_USERCONF="/cvmfs/grid.cern.ch/etc/grid-security/vomses"

    # fallback to a default path when the externally given software base is empty or inaccessible
    local fetch_software="true"
    if [ -z "${CF_SOFTWARE_BASE}" ]; then
        export CF_SOFTWARE_BASE="${CF_DATA}/software"
    elif [ ! -d "${CF_SOFTWARE_BASE}/conda" ] || [ ! -x "${CF_SOFTWARE_BASE}/conda" ]; then
        echo "software base directory ${CF_SOFTWARE_BASE} was configured to be shared,"
        echo "but conda subdirectory is either missing or not accessible"
        export CF_SOFTWARE_BASE="${CF_DATA}/software"
    else
        fetch_software="false"
        echo "found existing software at ${CF_SOFTWARE_BASE}"
    fi

    # when gfal is not available, check that the lcg_setup file exists
    local skip_lcg_setup="true"
    if ! type gfal-ls &> /dev/null; then
        ls "$( dirname "${lcg_setup}" )" &> /dev/null
        if [ ! -f "${lcg_setup}" ]; then
            >&2 echo "lcg setup file ${lcg_setup} not existing"
            return "1"
        fi
        skip_lcg_setup="false"
    fi

    # source the law wlcg tools, mainly for law_wlcg_get_file
    echo -e "\nsouring wlcg tools ..."
    source "${CF_WLCG_TOOLS}" "" || return "$?"
    echo "done sourcing wlcg tools"

    # load and unpack the software bundle, then source it
    if ${fetch_software}; then
        (
            echo -e "\nfetching software bundle ..."
            { ${skip_lcg_setup} || source "${lcg_setup}" ""; } &&
            mkdir -p "${CF_SOFTWARE_BASE}/conda" &&
            cd "${CF_SOFTWARE_BASE}/conda" &&
            GFAL_PYTHONBIN="$( which python3 )" law_wlcg_get_file '{{cf_software_uris}}' '{{cf_software_pattern}}' "software.tgz" &&
            tar -xzf "software.tgz" &&
            rm "software.tgz" &&
            echo "done fetching software bundle"
        ) || return "$?"
    fi

    # load the repo bundle
    (
        echo -e "\nfetching repository bundle ..."
        { ${skip_lcg_setup} || source "${lcg_setup}" ""; } &&
        mkdir -p "${CF_REPO_BASE}" &&
        cd "${CF_REPO_BASE}" &&
        GFAL_PYTHONBIN="$( which python3 )" law_wlcg_get_file '{{cf_repo_uris}}' '{{cf_repo_pattern}}' "repo.tgz" &&
        tar -xzf "repo.tgz" &&
        rm "repo.tgz" &&
        echo "done fetching repository bundle"
    ) || return "$?"

    # export variables used in cf setup script on-the-fly to load sandboxes
    if ! ${sharing_software}; then
        export CF_JOB_BASH_SANDBOX_URIS="{{cf_bash_sandbox_uris}}"
        export CF_JOB_BASH_SANDBOX_PATTERNS="{{cf_bash_sandbox_patterns}}"
        export CF_JOB_BASH_SANDBOX_NAMES="{{cf_bash_sandbox_names}}"
        export CF_JOB_CMSSW_SANDBOX_URIS="{{cf_cmssw_sandbox_uris}}"
        export CF_JOB_CMSSW_SANDBOX_PATTERNS="{{cf_cmssw_sandbox_patterns}}"
        export CF_JOB_CMSSW_SANDBOX_NAMES="{{cf_cmssw_sandbox_names}}"
    fi

    # optional custom command before the setup is sourced
    {{cf_pre_setup_command}}

    # source the default repo setup
    echo -e "\nsource repository setup ..."
    source "${CF_REPO_BASE}/setup.sh" "" || return "$?"
    echo "done sourcing repository setup"

    # optional custom command after the setup is sourced
    {{cf_post_setup_command}}

    return "0"
}


# Bootstrap function for slurm jobs.
bootstrap_slurm() {
    # set env variables
    export CF_ON_SLURM="1"
    export CF_REMOTE_ENV="1"
    export CF_REPO_BASE="{{cf_repo_base}}"
    export CF_WLCG_CACHE_ROOT="${LAW_JOB_HOME}/cf_wlcg_cache"
    export KRB5CCNAME="FILE:{{kerberosproxy_file}}"
    [ ! -z "{{vomsproxy_file}}" ] && export X509_USER_PROXY="{{vomsproxy_file}}"

    # optional custom command before the setup is sourced
    {{cf_pre_setup_command}}

    # source the default repo setup
    echo -e "\nsource repository setup ..."
    source "${CF_REPO_BASE}/setup.sh" "" || return "$?"
    echo "done sourcing repository setup"

    # optional custom command after the setup is sourced
    {{cf_post_setup_command}}
}


# Bootstrap function for crab jobs.
bootstrap_crab() {
    # set env variables
    export CF_ON_GRID="1"
    export CF_REMOTE_ENV="1"
    export CF_CERN_USER="{{cf_cern_user}}"
    export CF_REPO_BASE="${LAW_JOB_HOME}/repo"
    export CF_DATA="${LAW_JOB_HOME}/cf_data"
    export CF_SOFTWARE_BASE="${CF_DATA}/software"
    export CF_STORE_NAME="{{cf_store_name}}"
    export CF_WLCG_CACHE_ROOT="${LAW_JOB_HOME}/cf_wlcg_cache"
    export CF_WLCG_TOOLS="{{wlcg_tools}}"
    export LAW_CONFIG_FILE="{{law_config_file}}"
    local lcg_setup="{{cf_remote_lcg_setup}}"
    lcg_setup="${lcg_setup:-/cvmfs/grid.cern.ch/alma9-ui-test/etc/profile.d/setup-alma9-test.sh}"

    # when gfal is not available, check that the lcg_setup file exists
    local skip_lcg_setup="true"
    if ! type gfal-ls &> /dev/null; then
        ls "$( dirname "${lcg_setup}" )" &> /dev/null
        if [ ! -f "${lcg_setup}" ]; then
            >&2 echo "lcg setup file ${lcg_setup} not existing"
            return "1"
        fi
        skip_lcg_setup="false"
    fi

    # source the law wlcg tools, mainly for law_wlcg_get_file
    echo -e "\nsouring wlcg tools ..."
    source "${CF_WLCG_TOOLS}" "" || return "$?"
    echo "done sourcing wlcg tools"

    # load and unpack the software bundle, then source it
    (
        echo -e "\nfetching software bundle ..."
        { ${skip_lcg_setup} || source "${lcg_setup}" ""; } &&
        mkdir -p "${CF_SOFTWARE_BASE}/conda" &&
        cd "${CF_SOFTWARE_BASE}/conda" &&
        GFAL_PYTHONBIN="$( which python3 )" law_wlcg_get_file '{{cf_software_uris}}' '{{cf_software_pattern}}' "software.tgz" &&
        tar -xzf "software.tgz" &&
        rm "software.tgz" &&
        echo "done fetching software bundle"
    ) || return "$?"

    # load the repo bundle
    (
        echo -e "\nfetching repository bundle ..."
        { ${skip_lcg_setup} || source "${lcg_setup}" ""; } &&
        mkdir -p "${CF_REPO_BASE}" &&
        cd "${CF_REPO_BASE}" &&
        GFAL_PYTHONBIN="$( which python3 )" law_wlcg_get_file '{{cf_repo_uris}}' '{{cf_repo_pattern}}' "repo.tgz" &&
        tar -xzf "repo.tgz" &&
        rm "repo.tgz" &&
        echo "done fetching repository bundle"
    ) || return "$?"

    # export variables used in cf setup script on-the-fly to load sandboxes
    export CF_JOB_BASH_SANDBOX_URIS="{{cf_bash_sandbox_uris}}"
    export CF_JOB_BASH_SANDBOX_PATTERNS="{{cf_bash_sandbox_patterns}}"
    export CF_JOB_BASH_SANDBOX_NAMES="{{cf_bash_sandbox_names}}"
    export CF_JOB_CMSSW_SANDBOX_URIS="{{cf_cmssw_sandbox_uris}}"
    export CF_JOB_CMSSW_SANDBOX_PATTERNS="{{cf_cmssw_sandbox_patterns}}"
    export CF_JOB_CMSSW_SANDBOX_NAMES="{{cf_cmssw_sandbox_names}}"

    # optional custom command before the setup is sourced
    {{cf_pre_setup_command}}

    # source the default repo setup
    echo -e "\nsource repository setup ..."
    source "${CF_REPO_BASE}/setup.sh" "" || return "$?"
    echo "done sourcing repository setup"

    # optional custom command after the setup is sourced
    {{cf_post_setup_command}}

    return "0"
}

# job entry point
bootstrap_{{cf_bootstrap_name}} "$@"
