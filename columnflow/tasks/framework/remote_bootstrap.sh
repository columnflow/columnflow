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
    export CF_SOFTWARE_BASE="${CF_DATA}/software"
    export CF_CMSSW_BASE="${CF_DATA}/cmssw"
    export CF_STORE_NAME="{{cf_store_name}}"
    export CF_STORE_LOCAL="{{cf_store_local}}"
    export CF_LOCAL_SCHEDULER="{{cf_local_scheduler}}"
    export CF_WLCG_CACHE_ROOT="${LAW_JOB_HOME}/cf_wlcg_cache"
    export CF_VOMS="{{cf_voms}}"
    export CF_TASK_NAMESPACE="{{cf_task_namespace}}"
    export CF_LCG_SETUP="{{cf_lcg_setup}}"
    export X509_USER_PROXY="${PWD}/{{voms_proxy_file}}"

    # source the lcg software when defined
    if [ ! -z "${CF_LCG_SETUP}" ]; then
        source "${CF_LCG_SETUP}" "" || return "$?"
    fi

    # source the law wlcg tools, mainly for law_wlcg_get_file
    source "{{wlcg_tools}}" ""

    # load the software bundle
    (
        mkdir -p "${CF_SOFTWARE_BASE}" && \
        cd "${CF_SOFTWARE_BASE}" && \
        law_wlcg_get_file "{{cf_software_uris}}" "{{cf_software_pattern}}" "software.tgz" && \
        tar -xzf "software.tgz" && \
        rm "software.tgz"
    ) || return "$?"

    # load the repo bundle
    (
        mkdir -p "${CF_REPO_BASE}" && \
        cd "${CF_REPO_BASE}" && \
        law_wlcg_get_file "{{cf_repo_uris}}" "{{cf_repo_pattern}}" "repo.tgz" && \
        tar -xzf "repo.tgz" && \
        rm "repo.tgz"
    ) || return "$?"

    # prefetch cmssw sandbox bundles
    local cmssw_sandbox_uris={{cf_cmssw_sandbox_uris}}
    local cmssw_sandbox_patterns={{cf_cmssw_sandbox_patterns}}
    local cmssw_sandbox_names={{cf_cmssw_sandbox_names}}
    for (( i=0; i<${#cmssw_sandbox_uris[@]}; i+=1 )); do
        law_wlcg_get_file "${cmssw_sandbox_uris[i]}" "${cmssw_sandbox_patterns[i]}" "${CF_SOFTWARE_BASE}/cmssw_sandboxes/${cmssw_sandbox_names[i]}.tgz" || return "$?"
    done

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
    export CF_WLCG_CACHE_ROOT="${CF_WLCG_CACHE_ROOT:-${LAW_JOB_HOME}/cf_wlcg_cache}"
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
