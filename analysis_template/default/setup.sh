#!/usr/bin/env bash

setup___cf_prefix_lc__() {
    # Runs the project setup, leading to a collection of environment variables starting with either
    #   - "CF_", for controlling behavior implemented by columnflow, or
    #   - "__cf_prefix_uc___", for features provided by the analysis repository itself.
    # Check the setup.sh in columnflow for documentation of the "CF_" variables. The purpose of all
    # "__cf_prefix_uc___" variables is documented below.
    #
    # The setup also handles the installation of the software stack via virtual environments, and
    # optionally an interactive setup where the user can configure certain variables.
    #
    #
    # Arguments:
    #   1. The name of the setup. "default" (which is itself the default when no name is set)
    #      triggers a setup with good defaults, avoiding all queries to the user and the writing of
    #      a custom setup file. See "interactive_setup()" for more info.
    #
    #
    # Optinally preconfigured environment variables:
    #   None yet.
    #
    #
    # Variables defined by the setup and potentially required throughout the analysis:
    #   __cf_prefix_uc___BASE
    #       The absolute analysis base directory. Used to infer file locations relative to it.
    #   __cf_prefix_uc___SETUP
    #       A flag that is set to 1 after the setup was successful.

    # prevent repeated setups
    if [ "${__cf_prefix_uc___SETUP}" = "1" ]; then
        >&2 echo "the __cf_analysis_name__ analysis was already succesfully setup"
        >&2 echo "re-running the setup requires a new shell"
        return "1"
    fi


    #
    # prepare local variables
    #

    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local orig="${PWD}"
    local setup_name="${1:-default}"
    local setup_is_default="false"
    [ "${setup_name}" = "default" ] && setup_is_default="true"


    #
    # global variables
    # (__cf_prefix_uc__ = __cf_analysis_name__, CF = columnflow)
    #

    # start exporting variables
    export __cf_prefix_uc___BASE="${this_dir}"
    export CF_BASE="${this_dir}/modules/columnflow"
    export CF_REPO_BASE="${__cf_prefix_uc___BASE}"
    export CF_SETUP_NAME="${setup_name}"

    # load cf setup helpers
    CF_SKIP_SETUP="1" source "${CF_BASE}/setup.sh" "" || return "$?"

    # interactive setup
    if [ "${CF_REMOTE_JOB}" != "1" ]; then
        cf_setup_interactive_body() {
            # start querying for variables
            query CF_CERN_USER "CERN username" "$( whoami )"
            export_and_save CF_CERN_USER_FIRSTCHAR "\${CF_CERN_USER:0:1}"
            query CF_DATA "Local data directory" "\$__cf_prefix_uc___BASE/data" "./data"
            query CF_STORE_NAME "Relative path used in store paths (see next queries)" "__cf_prefix_lc___store"
            query CF_STORE_LOCAL "Default local output store" "\$CF_DATA/\$CF_STORE_NAME"
            query CF_WLCG_CACHE_ROOT "Local directory for caching remote files" "" "''"
            export_and_save CF_WLCG_USE_CACHE "$( [ -z "${CF_WLCG_CACHE_ROOT}" ] && echo false || echo true )"
            export_and_save CF_WLCG_CACHE_CLEANUP "${CF_WLCG_CACHE_CLEANUP:-false}"
            query CF_SOFTWARE_BASE "Local directory for installing software" "\$CF_DATA/software"
            query CF_JOB_BASE "Local directory for storing job files" "\$CF_DATA/jobs"
            query CF_VOMS "Virtual-organization" "cms"
            query CF_LOCAL_SCHEDULER "Use a local scheduler for law tasks" "True"
            if [ "${CF_LOCAL_SCHEDULER}" != "True" ]; then
                query CF_SCHEDULER_HOST "Address of a central scheduler for law tasks" "naf-cms15.desy.de"
                query CF_SCHEDULER_PORT "Port of a central scheduler for law tasks" "8082"
            else
                export_and_save CF_SCHEDULER_HOST "127.0.0.1"
                export_and_save CF_SCHEDULER_PORT "8082"
            fi
            query __cf_prefix_uc___BUNDLE_CMSSW "Install and bundle CMSSW sandboxes for job submission?" "True"
        }
        cf_setup_interactive "${CF_SETUP_NAME}" "${__cf_prefix_uc___BASE}/.setups/${CF_SETUP_NAME}.sh" || return "$?"
    fi

    # continue the fixed setup
    export CF_CONDA_BASE="${CF_CONDA_BASE:-${CF_SOFTWARE_BASE}/conda}"
    export CF_VENV_BASE="${CF_VENV_BASE:-${CF_SOFTWARE_BASE}/venvs}"
    export CF_CMSSW_BASE="${CF_CMSSW_BASE:-${CF_SOFTWARE_BASE}/cmssw}"
    export CF_CI_JOB="$( [ "${GITHUB_ACTIONS}" = "true" ] && echo 1 || echo 0 )"


    #
    # common variables
    #

    cf_setup_common_variables || return "$?"


    #
    # minimal local software setup
    #

    cf_setup_software_stack "${CF_SETUP_NAME}" || return "$?"

    # ammend paths that are not covered by the central cf setup
    export PATH="${__cf_prefix_uc___BASE}/bin:${PATH}"
    export PYTHONPATH="${__cf_prefix_uc___BASE}:${__cf_prefix_uc___BASE}/modules/cmsdb:${PYTHONPATH}"

    # initialze submodules
    if [ -e "${__cf_prefix_uc___BASE}/.git" ]; then
        local m
        for m in $( ls -1q "${__cf_prefix_uc___BASE}/modules" ); do
            cf_init_submodule "${__cf_prefix_uc___BASE}" "modules/${m}"
        done
    fi


    #
    # law setup
    #

    export LAW_HOME="${__cf_prefix_uc___BASE}/.law"
    export LAW_CONFIG_FILE="${__cf_prefix_uc___BASE}/law.cfg"

    if which law &> /dev/null; then
        # source law's bash completion scipt
        source "$( law completion )" ""

        # silently index
        law index -q
    fi

    # finalize
    export __cf_prefix_uc___SETUP="1"
}

main() {
    # Invokes the main action of this script, catches possible error codes and prints a message.

    # run the actual setup
    if setup___cf_prefix_lc__ "$@"; then
        cf_color green "__cf_analysis_name__ analysis successfully setup"
        return "0"
    else
        local code="$?"
        cf_color red "setup failed with code ${code}"
        return "${code}"
    fi
}

# entry point
if [ "${__cf_prefix_uc___SKIP_SETUP}" != "1" ]; then
    main "$@"
fi
