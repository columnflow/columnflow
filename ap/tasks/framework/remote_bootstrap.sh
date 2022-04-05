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
    export AP_USER="{{ap_user}}"
    export AP_BASE="$LAW_JOB_HOME/repo"
    export AP_DATA="$LAW_JOB_HOME/ap_data"
    export AP_SOFTWARE="$AP_DATA/software"
    export AP_STORE_NAME="{{ap_store_name}}"
    export AP_STORE_LOCAL="$AP_DATA/store"
    export AP_WLCG_USE_CACHE="false"
    export AP_LOCAL_SCHEDULER="{{ap_local_scheduler}}"
    export AP_ON_HTCONDOR="1"
    export AP_REMOTE_JOB="1"
    export AP_REMOTE_NEWENV="1"
    export X509_USER_PROXY="$PWD/{{ap_proxy_file}}{{file_postfix}}"

    # source the lcg software for access to wlcg executables
    source "{{ap_lcg_dir}}/etc/profile.d/setup-c7-ui-example.sh" "" || return "$?"

    # source the law wlcg tools, mainly for law_wlcg_get_file
    source "law_wlcg_tools{{file_postfix}}.sh" ""

    # load the software bundle
    (
        mkdir -p "$AP_SOFTWARE"
        cd "$AP_SOFTWARE"
        law_wlcg_get_file "{{ap_software_uris}}" "{{ap_software_pattern}}" "software.tgz" || return "$?"
        tar -xzf "software.tgz" || return "$?"
        rm "software.tgz"
    ) || return "$?"

    # load the repo bundle
    (
        mkdir -p "$AP_BASE"
        cd "$AP_BASE"
        law_wlcg_get_file "{{ap_repo_uris}}" "{{ap_repo_pattern}}" "repo.tgz" || return "$?"
        tar -xzf "repo.tgz" || return "$?"
        rm "repo.tgz"
    ) || return "$?"

    # prefetch cmssw sandbox bundles
    local cmssw_sandbox_uris={{ap_cmssw_sandbox_uris}}
    local cmssw_sandbox_patterns={{ap_cmssw_sandbox_patterns}}
    local cmssw_sandbox_names={{ap_cmssw_sandbox_names}}
    for (( i=0; i<${#cmssw_sandbox_uris[@]}; i+=1 )); do
        law_wlcg_get_file "${cmssw_sandbox_uris[i]}" "${cmssw_sandbox_patterns[i]}" "$AP_SOFTWARE/cmssw_sandboxes/${cmssw_sandbox_names[i]}.tgz" || return "$?"
    done

    # source the repo setup
    source "$AP_BASE/setup.sh" "default" || return "$?"

    return "0"
}

bootstrap_{{ap_bootstrap_name}} "$@"
