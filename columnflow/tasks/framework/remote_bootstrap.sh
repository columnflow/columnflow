#!/usr/bin/env bash

# Bootstrap file that is executed in remote jobs submitted by law to set up the environment.
# So-called render variables, denoted by "{{name}}", are replaced with variables configured in the
# remote workflow tasks, e.g. in HTCondorWorkflow.htcondor_job_config() upon job submission.

# Bootstrap function for standalone htcondor jobs.
bootstrap_htcondor_standalone() {
    # set env variables
    export CF_ON_HTCONDOR="1"
    export CF_REMOTE_JOB="1"
    export CF_CERN_USER="{{cf_cern_user}}"
    export CF_REPO_BASE="${LAW_JOB_HOME}/repo"
    export CF_DATA="${LAW_JOB_HOME}/cf_data"
    export CF_SOFTWARE_BASE="{{cf_software_base}}"
    export CF_STORE_NAME="{{cf_store_name}}"
    export CF_STORE_LOCAL="{{cf_store_local}}"
    export CF_LOCAL_SCHEDULER="{{cf_local_scheduler}}"
    export CF_WLCG_CACHE_ROOT="${LAW_JOB_HOME}/cf_wlcg_cache"
    export CF_VOMS="{{cf_voms}}"
    export CF_TASK_NAMESPACE="{{cf_task_namespace}}"
    export X509_USER_PROXY="${PWD}/{{voms_proxy_file}}"

    # fallback to a default path when the externally given software base is empty or inaccessible
    local fetch_software="true"
    if [ -z "${CF_SOFTWARE_BASE}" ] || [ ! -d "${CF_SOFTWARE_BASE}/conda" ] || [ ! -x "${CF_SOFTWARE_BASE}/conda" ]; then
        export CF_SOFTWARE_BASE="${CF_DATA}/software"
    else
        fetch_software="false"
        echo "found existing software at ${CF_SOFTWARE_BASE}"
    fi

    # source the law wlcg tools, mainly for law_wlcg_get_file
    local lcg_setup="/cvmfs/grid.cern.ch/centos7-ui-160522/etc/profile.d/setup-c7-ui-python3-example.sh"
    source "{{wlcg_tools}}" "" || return "$?"

    # load and unpack the software bundle, then source it
    if ${fetch_software}; then
        (
            source "${lcg_setup}" "" &&
            mkdir -p "${CF_SOFTWARE_BASE}/conda" &&
            cd "${CF_SOFTWARE_BASE}/conda" &&
            law_wlcg_get_file "{{cf_software_uris}}" "{{cf_software_pattern}}" "software.tgz" &&
            tar -xzf "software.tgz" &&
            rm "software.tgz"
        ) || return "$?"
    fi

    # load the repo bundle
    (
        source "${lcg_setup}" "" &&
        mkdir -p "${CF_REPO_BASE}" &&
        cd "${CF_REPO_BASE}" &&
        law_wlcg_get_file "{{cf_repo_uris}}" "{{cf_repo_pattern}}" "repo.tgz" &&
        tar -xzf "repo.tgz" &&
        rm "repo.tgz"
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
    source "${CF_REPO_BASE}/setup.sh" "" || return "$?"

    # optional custom command after the setup is sourced
    {{cf_post_setup_command}}

    return "0"
}


# Bootstrap function for slurm jobs.
bootstrap_slurm() {
    # set env variables
    export CF_ON_SLURM="1"
    export CF_REMOTE_JOB="1"
    export CF_REPO_BASE="{{cf_repo_base}}"
    export CF_WLCG_CACHE_ROOT="${LAW_JOB_HOME}/cf_wlcg_cache"
    export X509_USER_PROXY="{{voms_proxy_file}}"
    export KRB5CCNAME="FILE:{{kerberos_proxy_file}}"

    # optional custom command before the setup is sourced
    {{cf_pre_setup_command}}

    # source the default repo setup
    source "${CF_REPO_BASE}/setup.sh" "" || return "$?"

    # optional custom command after the setup is sourced
    {{cf_post_setup_command}}
}


# job entry point
bootstrap_{{cf_bootstrap_name}} "$@"
