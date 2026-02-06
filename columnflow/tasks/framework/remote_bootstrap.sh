#!/usr/bin/env bash

# Bootstrap file that is executed in remote jobs submitted by law to set up the environment.
# So-called render variables, denoted by "{{name}}", are replaced with variables configured in the
# remote workflow tasks, e.g. in HTCondorWorkflow.htcondor_job_config() upon job submission.

# Bootstrap function for standalone htcondor jobs.
bootstrap_htcondor_standalone() {
    # set env variables
    export CF_REMOTE_ENV="true"
    export CF_ON_HTCONDOR="true"
    export CF_HTCONDOR_FLAVOR="{{cf_htcondor_flavor}}"
    export CF_CERN_USER="{{cf_cern_user}}"
    export CF_CERN_USER_FIRSTCHAR="${CF_CERN_USER:0:1}"
    export CF_REPO_BASE="${LAW_JOB_HOME}/repo"
    export CF_DATA="${LAW_JOB_HOME}/cf_data"
    export CF_SOFTWARE_BASE="${CF_DATA}/software"
    export CF_CONDA_BASE="{{cf_conda_base}}"
    export CF_VENV_BASE="{{cf_venv_base}}"
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
        if [ ! -f "${tmp_x509}" ]; then
            cp "${X509_USER_PROXY}" "${tmp_x509}" || {
                >&2 echo "could not copy X509_USER_PROXY file from ${X509_USER_PROXY} to ${tmp_x509}, stopping job"
                return "1"
            }
        fi
    fi
    local share_software="{{cf_htcondor_share_software}}"
    share_software="${share_software:-false}"
    local lcg_setup="{{cf_remote_lcg_setup}}"
    lcg_setup="${lcg_setup:-/cvmfs/grid.cern.ch/alma9-ui-test/etc/profile.d/setup-alma9-test.sh}"
    local force_lcg_setup="$( [ -z "{{cf_remote_lcg_setup_force}}" ] && echo "false" || echo "true" )"

    # fix for missing voms/x509 variables in the lcg setup of the naf
    if [[ "${CF_HTCONDOR_FLAVOR}" = naf* ]]; then
        echo "setting up X509 variables for ${CF_HTCONDOR_FLAVOR}"
        export X509_CERT_DIR="/cvmfs/grid.cern.ch/etc/grid-security/certificates"
        export X509_VOMS_DIR="/cvmfs/grid.cern.ch/etc/grid-security/vomsdir"
        export X509_VOMSES="/cvmfs/grid.cern.ch/etc/grid-security/vomses"
        export VOMS_USERCONF="/cvmfs/grid.cern.ch/etc/grid-security/vomses"
        export CAPATH="/cvmfs/grid.cern.ch/etc/grid-security/certificates"
    fi

    # prepare sharing of conda and venv setup
    local fetch_conda_bundle="true"
    if ${share_software}; then
        # check conda
        if [ -z "${CF_CONDA_BASE}" ]; then
            >&2 echo "cf_htcondor_share_software is set to true, but cf_conda_base is empty"
            return "1"
        fi
        if [ ! -d "${CF_CONDA_BASE}" ] || [ ! -x "${CF_CONDA_BASE}" ]; then
            >&2 echo "cf_htcondor_share_software is set to true, but cf_conda_base is not accessible: ${CF_CONDA_BASE}"
            return "1"
        fi
        fetch_conda_bundle="false"
        echo "detected existing conda setup at ${CF_CONDA_BASE}"

        # check venvs
        if [ -z "${CF_VENV_BASE}" ]; then
            >&2 echo "cf_htcondor_share_software is set to true, but cf_venv_base is empty"
            return "1"
        fi
        if [ ! -d "${CF_VENV_BASE}" ] || [ ! -x "${CF_VENV_BASE}" ]; then
            >&2 echo "cf_htcondor_share_software is set to true, but cf_venv_base is not accessible: ${CF_VENV_BASE}"
            return "1"
        fi
        # set variables required by _setup_venv script to load and setup venvs on-the-fly
        export CF_JOB_BASH_SANDBOX_URIS="{{cf_bash_sandbox_uris}}"
        export CF_JOB_BASH_SANDBOX_PATTERNS="{{cf_bash_sandbox_patterns}}"
        export CF_JOB_BASH_SANDBOX_NAMES="{{cf_bash_sandbox_names}}"
        export CF_JOB_CMSSW_SANDBOX_URIS="{{cf_cmssw_sandbox_uris}}"
        export CF_JOB_CMSSW_SANDBOX_PATTERNS="{{cf_cmssw_sandbox_patterns}}"
        export CF_JOB_CMSSW_SANDBOX_NAMES="{{cf_cmssw_sandbox_names}}"
        echo "detected existing venvs at ${CF_VENV_BASE}"
    else
        # force local bases
        export CF_CONDA_BASE="${CF_SOFTWARE_BASE}/conda"
        export CF_VENV_BASE="${CF_SOFTWARE_BASE}/venvs"
    fi

    # when gfal is not available, check that the lcg_setup file exists
    local skip_lcg_setup="true"
    if ${force_lcg_setup} || ! type gfal-ls &> /dev/null; then
        # stat the setup file with a timeout to avoid hanging
        timeout 20 stat "${lcg_setup}" &> /dev/null
        local ret="$?"
        if [ "${ret}" = "124" ]; then
            >&2 echo "lcg setup file ${lcg_setup} not accessible, mount not responding after 20s"
            return "1"
        elif [ "${ret}" != "0" ]; then
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
    if ${fetch_conda_bundle}; then
        (
            echo -e "\nfetching software bundle ..."
            { ${skip_lcg_setup} || source "${lcg_setup}" ""; } &&
            mkdir -p "${CF_CONDA_BASE}" &&
            cd "${CF_CONDA_BASE}" &&
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
    export CF_REMOTE_ENV="true"
    export CF_ON_SLURM="true"
    export CF_SLURM_FLAVOR="{{cf_slurm_flavor}}"
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
    export CF_ON_GRID="true"
    export CF_REMOTE_ENV="true"
    export CF_CERN_USER="{{cf_cern_user}}"
    export CF_CERN_USER_FIRSTCHAR="${CF_CERN_USER:0:1}"
    export CF_REPO_BASE="${LAW_JOB_HOME}/repo"
    export CF_DATA="${LAW_JOB_HOME}/cf_data"
    export CF_SOFTWARE_BASE="${CF_DATA}/software"
    export CF_CONDA_BASE="${CF_SOFTWARE_BASE}/conda"
    export CF_VENV_BASE="${CF_SOFTWARE_BASE}/venvs"
    export CF_STORE_NAME="{{cf_store_name}}"
    export CF_STORE_LOCAL="${CF_DATA}/${CF_STORE_NAME}"
    export CF_WLCG_CACHE_ROOT="${LAW_JOB_HOME}/cf_wlcg_cache"
    export CF_WLCG_TOOLS="{{wlcg_tools}}"
    export LAW_CONFIG_FILE="{{law_config_file}}"
    local lcg_setup="{{cf_remote_lcg_setup}}"
    lcg_setup="${lcg_setup:-/cvmfs/grid.cern.ch/alma9-ui-test/etc/profile.d/setup-alma9-test.sh}"
    local force_lcg_setup="$( [ -z "{{cf_remote_lcg_setup_force}}" ] && echo "false" || echo "true" )"

    # when gfal is not available, check that the lcg_setup file exists
    local skip_lcg_setup="true"
    if ${force_lcg_setup} || ! type gfal-ls &> /dev/null; then
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
        mkdir -p "${CF_CONDA_BASE}" &&
        cd "${CF_CONDA_BASE}" &&
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

    # log all CF variables for debugging
    # python -c 'import os; print("\n".join(f"{k} = {v}" for k, v in os.environ.items() if k.startswith("CF_")))'

    # optional custom command after the setup is sourced
    {{cf_post_setup_command}}

    return "0"
}

# helper to remove fragments from ":"-separated path variables using expressions
filter_path_var() {
    # get arguments
    local old_val="$1"
    shift
    local regexps
    regexps=( ${@} )

    # loop through paths and set the new variable if no expression matched
    local new_val=""
    printf '%s:\0' "${old_val}" | while IFS=: read -d: -r p; do
        local matched="false"
        local regexp
        for regexp in ${regexps[@]}; do
            if echo "${p}" | grep -Po "${regexp}" &> /dev/null; then
                matched="true"
                break
            fi
        done
        if ! ${matched}; then
            [ ! -z "${new_val}" ] && new_val="${new_val}:"
            new_val="${new_val}${p}"
            echo "${new_val}"
        fi
    done | tail -n 1
}

# job entry point
bootstrap_{{cf_bootstrap_name}} "$@"
