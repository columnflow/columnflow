#!/usr/bin/env bash

# Bootstrap file that is executed in remote jobs submitted by law to set up the environment.
# So-called render variables, denoted by "{{name}}", are replaced with variables configured in the
# remote workflow tasks, e.g. in HTCondorWorkflow.htcondor_job_config() upon job submission.

# Bootstrap function for standalone htcondor jobs, i.e., each jobs fetches a software and repository
# code bundle and unpacks them to have a standalone environment, independent of the submitting one.
# The setup script of the repository is sourced with a few environment variables being set before,
# tailored for remote jobs.
bootstrap_htcondor_standalone() {
    # set env variables
    export CF_ON_HTCONDOR="1"
    export CF_REMOTE_JOB="1"
    export CF_DESY_USER="{{cf_desy_user}}"
    export CF_CERN_USER="{{cf_cern_user}}"
    export CF_BASE="$LAW_JOB_HOME/repo"
    export CF_DATA="$LAW_JOB_HOME/cf_data"
    export CF_SOFTWARE="$CF_DATA/software"
    export CF_STORE_NAME="{{cf_store_name}}"
    export CF_STORE_LOCAL="{{cf_store_local}}"
    export CF_LOCAL_SCHEDULER="{{cf_local_scheduler}}"
    export CF_LCG_SETUP="{{cf_lcg_setup}}"
    export X509_USER_PROXY="$PWD/{{proxy_file}}"

    # source the lcg software when defined
    if [ ! -z "$CF_LCG_SETUP" ]; then
        source "$CF_LCG_SETUP" "" || return "$?"
    fi

    # source the law wlcg tools, mainly for law_wlcg_get_file
    source "{{wlcg_tools}}" ""

    # load the software bundle
    (
        mkdir -p "$CF_SOFTWARE"
        cd "$CF_SOFTWARE"
        law_wlcg_get_file "{{cf_software_uris}}" "{{cf_software_pattern}}" "software.tgz" || return "$?"
        tar -xzf "software.tgz" || return "$?"
        rm "software.tgz"
    ) || return "$?"

    # load the repo bundle
    (
        mkdir -p "$CF_BASE"
        cd "$CF_BASE"
        law_wlcg_get_file "{{cf_repo_uris}}" "{{cf_repo_pattern}}" "repo.tgz" || return "$?"
        tar -xzf "repo.tgz" || return "$?"
        rm "repo.tgz"
    ) || return "$?"

    # prefetch cmssw sandbox bundles
    local cmssw_sandbox_uris={{cf_cmssw_sandbox_uris}}
    local cmssw_sandbox_patterns={{cf_cmssw_sandbox_patterns}}
    local cmssw_sandbox_names={{cf_cmssw_sandbox_names}}
    for (( i=0; i<${#cmssw_sandbox_uris[@]}; i+=1 )); do
        law_wlcg_get_file "${cmssw_sandbox_uris[i]}" "${cmssw_sandbox_patterns[i]}" "$CF_SOFTWARE/cmssw_sandboxes/${cmssw_sandbox_names[i]}.tgz" || return "$?"
    done

    # source the default repo setup
    source "$CF_BASE/setup.sh" "" || return "$?"

    return "0"
}

bootstrap_{{cf_bootstrap_name}} "$@"
