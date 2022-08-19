#!/usr/bin/env bash

setup_columnflow() {
    # Runs the entire project setup, leading to a collection of environment variables starting with
    # "CF_", the installation of the software stack via virtual environments, and optionally an
    # interactive setup where the user can configure certain variables.
    #
    #
    # Arguments:
    #   1. The name of the setup. "default" (which is itself the default when no name is set)
    #      triggers a setup with good defaults, avoiding all queries to the user and the writing of
    #      a custom setup file. See "interactive_setup()" for more info.
    #
    #
    # Optionally preconfigured environment variables:
    #   CF_REINSTALL_SOFTWARE
    #       When "1", any existing software stack is removed and freshly installed.
    #   CF_REMOTE_JOB
    #       When "1", applies configurations for remote job. Remote jobs will set this value if needed
    #       and there is no need to set this by hand.
    #   CF_LCG_SETUP
    #       The location of a custom LCG software setup file.
    #   X509_USER_PROXY
    #       A custom globus user proxy location.
    #   LANGUAGE, LANG, LC_ALL
    #       Custom language flags.
    #
    #
    # Variables defined by the setup and potentially required throughout columnflow:
    #   CF_SETUP_NAME
    #       The name of the setup. See above.
    #   CF_BASE
    #       The absolute columnflow base directory. Used to infer file locations relative to it.
    #   CF_DATA
    #       The main data directory where outputs and software can be stored in. Internally, this
    #       serves as a default for e.g. $CF_SOFTWARE_BASE, $CF_CMSSW_BASE, $CF_JOB_BASE and
    #       $CF_VENV_BASE which can, however, potentially point to a different directory. Queried
    #       during the interactive setup.
    #   CF_SOFTWARE_BASE
    #       The directory where general software is installed. Might point to $CF_DATA/software.
    #       Queried during the interactive setup.
    #   CF_CMSSW_BASE
    #       The directory where CMSSW releases are installed. Might point to $CF_DATA/cmssw. Queried
    #       during the interactive setup.
    #   CF_VENV_BASE
    #       The directory where virtual envs are installed. Might point to $CF_SOFTWARE_BASE/venvs.
    #       Queried during the interactive setup.
    #   CF_JOB_BASE
    #       The directory where job files from batch system submissions are kept. Used in law.cfg.
    #       Might point to $CF_DATA/jobs. Queried during the interactive setup.
    #   CF_STORE_NAME
    #       The name (not path) of store directories that will be created to contain output targets,
    #       potentially both locally and remotely. Queried during the interactive setup.
    #   CF_STORE_LOCAL
    #       The default local output store path, ammended by $CF_STORE_NAME. Queried during the
    #       interactive setup.
    #   CF_CI_JOB
    #       Set to "1" if a CI environment is detected (e.g. GitHub actions), and "0" otherwise.
    #   CF_VOMS
    #       The name of the user's virtual organization. Queried during the interactive setup.
    #   CF_LCG_SETUP
    #       The location of a custom LCG software setup file. See above.
    #   CF_ORIG_PATH
    #       Copy of the $PATH variable before ammended by the seutp.
    #   CF_ORIG_PYTHONPATH
    #       Copy of the $PYTHONPATH variable before ammended by the seutp.
    #   CF_ORIG_PYTHON3PATH
    #       Copy of the $PYTHON3PATH variable before ammended by the seutp.
    #   CF_ORIG_LD_LIBRARY_PATH
    #       Copy of the $LD_LIBRARY_PATH variable before ammended by the seutp.
    #   CF_WLCG_CACHE_ROOT
    #       The directory in which remote files from WLCG locations might be cached. No caching is
    #       used when empty. Queried during the interactive setup. Used in law.cfg.
    #   CF_WLCG_USE_CACHE
    #       Saves the decision on whether WLCG file caching is used or not, based on
    #       $CF_WLCG_CACHE_ROOT. Set to "true" when a remote job environment is detected. Used in
    #       law.cfg.
    #   CF_WLCG_CACHE_CLEANUP
    #       Set to "false" when the variable is not already existing. When "false", caches are not
    #       cleared after programs terminate and become persistent. Set to "true" when a remove job
    #       environment is detected. Used in law.cfg.
    #   CF_CERN_USER
    #       The user's CERN / WLCG name. Used in law.cfg.
    #   CF_CERN_USER_FIRSTCHAR
    #       The first character of the user's CERN / WLCG name. Derived from $CF_CERN_USER. Used in
    #       law.cfg.
    #   CF_TASK_NAMESPACE
    #       The prefix (namespace) of tasks in columnflow. Set to "cf" when not already defined.
    #   CF_LOCAL_SCHEDULER
    #       Either "true" or "false", deciding whether the process-local luigi scheduler should be
    #       used by default. Queried during the interactive setup. Used in law.cfg.
    #   CF_SCHEDULER_HOST
    #       Set to "127.0.0.1" when $CF_LOCAL_SCHEDULER was set to "true", or otherwise queried
    #       during the interactive setup. Used in law.cfg.
    #   CF_SCHEDULER_PORT
    #       Set to "8082" when $CF_LOCAL_SCHEDULER was set to "true", or otherwise queried during
    #       the interactive setup. Used in law.cfg.
    #   CF_WORKER_KEEP_ALIVE
    #       Set to "false" when not already set, or when a remote ($CF_REMOTE_JOB) or CI environment
    #       ($CF_CI_JOB) is detected. Used in law.cfg.
    #   PATH
    #       Ammended PATH variable.
    #   PYTHONPATH
    #       Ammended PYTHONPATH variable.
    #   PYTHONWARNINGS
    #       Set to "ignore".
    #   GLOBUS_THREAD_MODEL
    #       Set to "none".
    #   VIRTUAL_ENV_DISABLE_PROMPT
    #       Set to "1" when not defined already, leading to virtual envs leaving the PS1 prompt
    #       variable unaltered.
    #   X509_USER_PROXY
    #       Set to "/tmp/x509up_u$( id -u )" if not already set.
    #   LAW_HOME
    #       Set to $CF_BASE/.law.
    #   LAW_CONFIG_FILE
    #       Set to $CF_BASE/law.cfg.

    #
    # prepare local variables
    #

    local shell_is_zsh=$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local orig="${PWD}"
    local setup_name="${1:-default}"
    local setup_is_default="false"
    [ "${setup_name}" = "default" ] && setup_is_default="true"


    #
    # global variables
    # (CF = columnflow)
    #

    # lang defaults
    export LANGUAGE="${LANGUAGE:-en_US.UTF-8}"
    export LANG="${LANG:-en_US.UTF-8}"
    export LC_ALL="${LC_ALL:-en_US.UTF-8}"

    # proxy
    export X509_USER_PROXY="${X509_USER_PROXY:-/tmp/x509up_u$( id -u )}"

    # start exporting variables
    export CF_BASE="${this_dir}"
    export CF_SETUP_NAME="${setup_name}"

    # interactive setup
    cf_setup_interactive_body() {
        # start querying for variables
        query CF_CERN_USER "CERN username" "$( whoami )"
        export_and_save CF_CERN_USER_FIRSTCHAR "\${CF_CERN_USER:0:1}"
        query CF_DATA "Local data directory" "\$CF_BASE/data" "./data"
        query CF_STORE_NAME "Relative path used in store paths (see next queries)" "cf_store"
        query CF_STORE_LOCAL "Default local output store" "\$CF_DATA/\$CF_STORE_NAME"
        query CF_WLCG_CACHE_ROOT "Local directory for caching remote files" "" "''"
        export_and_save CF_WLCG_USE_CACHE "$( [ -z "${CF_WLCG_CACHE_ROOT}" ] && echo false || echo true )"
        export_and_save CF_WLCG_CACHE_CLEANUP "${CF_WLCG_CACHE_CLEANUP:-false}"
        query CF_SOFTWARE_BASE "Local directory for installing software" "\$CF_DATA/software"
        query CF_CMSSW_BASE "Local directory for installing CMSSW" "\$CF_DATA/cmssw"
        query CF_JOB_BASE "Local directory for storing job files" "\$CF_DATA/jobs"
        query CF_LOCAL_SCHEDULER "Use a local scheduler for law tasks" "True"
        if [ "${CF_LOCAL_SCHEDULER}" != "True" ]; then
            query CF_SCHEDULER_HOST "Address of a central scheduler for law tasks" "naf-cms15.desy.de"
            query CF_SCHEDULER_PORT "Port of a central scheduler for law tasks" "8082"
        else
            export_and_save CF_SCHEDULER_HOST "127.0.0.1"
            export_and_save CF_SCHEDULER_PORT "8082"
        fi
        query CF_VOMS "Virtual-organization" "cms:/cms/dcms"
        export_and_save CF_TASK_NAMESPACE "${CF_TASK_NAMESPACE:-cf}"
    }
    cf_setup_interactive "${CF_SETUP_NAME}" "${CF_BASE}/.setups/${CF_SETUP_NAME}.sh" || return "$?"

    # continue the fixed setup
    export CF_VENV_BASE="${CF_SOFTWARE_BASE}/venvs"
    export CF_ORIG_PATH="${PATH}"
    export CF_ORIG_PYTHONPATH="${PYTHONPATH}"
    export CF_ORIG_PYTHON3PATH="${PYTHON3PATH}"
    export CF_ORIG_LD_LIBRARY_PATH="${LD_LIBRARY_PATH}"
    export CF_CI_JOB="$( [ "${GITHUB_ACTIONS}" = "true" ] && echo 1 || echo 0 )"

    # overwrite some variables in remote and ci jobs
    if [ "${CF_REMOTE_JOB}" = "1" ]; then
        export CF_WLCG_USE_CACHE="true"
        export CF_WLCG_CACHE_CLEANUP="true"
        export CF_WORKER_KEEP_ALIVE="false"
    elif [ "${CF_CI_JOB}" = "1" ]; then
        export CF_WORKER_KEEP_ALIVE="false"
    fi

    # some variable defaults
    [ -z "${CF_WORKER_KEEP_ALIVE}" ] && export CF_WORKER_KEEP_ALIVE="false"


    #
    # minimal local software setup
    #

    cf_setup_software_stack "${CF_SETUP_NAME}" || return "$?"


    #
    # law setup
    #

    export LAW_HOME="${CF_BASE}/.law"
    export LAW_CONFIG_FILE="${CF_BASE}/law.cfg"

    if which law &> /dev/null; then
        # source law's bash completion scipt
        source "$( law completion )" ""

        # silently index
        law index -q
    fi
}

cf_setup_interactive() {
    # Starts the interactive part of the setup by querying for values of certain environment
    # variables with useful defaults. When a custom, named setup is triggered, the values of all
    # queried environment variables are stored in a file.
    #
    # The actual variables that should be queried and exported during the setup are to be defined
    # beforehand a function called "cf_setup_interactive_body". This function can make use of the
    # internal functions "export_and_save" and "query", defined below.
    #
    #
    # Arguments:
    #   1. The name of the setup. "default" triggers a setup with good defaults, avoiding all
    #      queries to the user and the writing of a custom setup file.
    #   2. The location of the setup file when a custom, named setup was triggered.

    local setup_name="${1}"
    local env_file="${2}"
    local env_file_tmp="${env_file}.tmp"
    local setup_is_default="false"
    [ "${setup_name}" = "default" ] && setup_is_default="true"

    # when the setup already exists and it's not the default one,
    # source the corresponding env file and stop
    if ! ${setup_is_default}; then
        if [ -f "${env_file}" ]; then
            echo -e "using variables for setup '\x1b[0;49;35m${setup_name}\x1b[0m' from ${env_file}"
            source "${env_file}" ""
            return "$?"
        elif [ -z "${env_file}" ]; then
            >&2 echo "no env file passed as 2nd argument to cf_interactive_setup"
            return "1"
        else
            echo -e "no setup file ${env_file} found for setup '\x1b[0;49;35m${setup_name}\x1b[0m'"
        fi
    fi

    export_and_save() {
        local varname="$1"
        local value="$2"

        export $varname="$( eval "echo ${value}" )"
        if ! ${setup_is_default}; then
            echo "export ${varname}=\"${value}\"" >> "${env_file_tmp}"
        fi
    }

    query() {
        local varname="$1"
        local text="$2"
        local default="$3"
        local default_text="${4:-$default}"
        local default_raw="${default}"

        # when the setup is the default one, use the default value when the env variable is empty,
        # otherwise, query interactively
        local value="${default}"
        if ${setup_is_default}; then
            # set the variable when existing
            eval "value=\${$varname:-\$value}"
        else
            printf "${text} (\x1b[1;49;39m${varname}\x1b[0m, default \x1b[1;49;39m${default_text}\x1b[0m):  "
            read query_response
            [ "X${query_response}" = "X" ] && query_response="${default}"

            # repeat for boolean flags that were not entered correctly
            while true; do
                ( [ "${default}" != "True" ] && [ "${default}" != "False" ] ) && break
                ( [ "${query_response}" = "True" ] || [ "${query_response}" = "False" ] ) && break
                printf "please enter either '\x1b[1;49;39mTrue\x1b[0m' or '\x1b[1;49;39mFalse\x1b[0m':  " query_response
                read query_response
                [ "X${query_response}" = "X" ] && query_response="${default}"
            done
            value="${query_response}"

            # strip " and ' on both sides
            value=${value%\"}
            value=${value%\'}
            value=${value#\"}
            value=${value#\'}
        fi

        export_and_save "${varname}" "${value}"
    }

    # prepare the tmp env file
    if ! ${setup_is_default}; then
        rm -f "${env_file_tmp}"
        mkdir -p "$( dirname "${env_file_tmp}" )"

        echo -e "Start querying variables for setup '\x1b[0;49;35m${setup_name}\x1b[0m', press enter to accept default values\n"
    fi

    # query for variables
    cf_setup_interactive_body || return "$?"

    # move the env file to the correct location for later use
    if ! ${setup_is_default}; then
        mv "${env_file_tmp}" "${env_file}"
        echo -e "\nvariables written to ${env_file}"
    fi
}

cf_setup_software_stack() {
    # Sets up the columnflow software stack consisting of updated PATH variables, virtual
    # environments and git submodule initialization / updates.
    #
    #
    # Arguments:
    #   1. setup_name
    #       The name of the setup.
    #
    #
    # Required environment variables:
    #   CF_BASE
    #       The columnflow base directory.
    #   CF_VENV_BASE
    #       The base directory were virtual envs are installed.
    #
    #
    # Optional environments variables:
    #   CF_CI_JOB
    #       When "1", the software stack is sourcd but not built.
    #   CF_REINSTALL_SOFTWARE
    #       When "1", any existing software stack is removed and freshly installed.

    local setup_name="${1}"
    local setup_is_default="false"
    [ "${setup_name}" = "default" ] && setup_is_default="true"

    # use the latest centos7 ui from the grid setup on cvmfs
    [ -z "${CF_LCG_SETUP}" ] && export CF_LCG_SETUP="/cvmfs/grid.cern.ch/centos7-ui-160522/etc/profile.d/setup-c7-ui-python3-example.sh"
    if [ -f "${CF_LCG_SETUP}" ]; then
        source "${CF_LCG_SETUP}" ""
    elif [ "${CF_CI_JOB}" = "1" ]; then
        >&2 echo "LCG setup file ${CF_LCG_SETUP} not existing in CI env, skipping"
    else
        >&2 echo "LCG setup file ${CF_LCG_SETUP} not existing"
        return "1"
    fi

    # update paths and flags
    local pyv="$( python3 -c "import sys; print('{0.major}.{0.minor}'.format(sys.version_info))" )"
    export PATH="${CF_BASE}/bin:${CF_BASE}/modules/law/bin:${CF_SOFTWARE_BASE}/bin:${PATH}"
    export PYTHONPATH="${CF_BASE}:${CF_BASE}/modules/law:${CF_BASE}/modules/order:${PYTHONPATH}"
    export PYTHONWARNINGS="ignore"
    export GLOBUS_THREAD_MODEL="none"
    [ -z "${VIRTUAL_ENV_DISABLE_PROMPT}" ] && export VIRTUAL_ENV_DISABLE_PROMPT="1"
    ulimit -s unlimited

    # local python stack in two virtual envs:
    # - "cf_prod": contains the minimal stack to run tasks and is sent alongside jobs
    # - "cf_dev" : "prod" + additional python tools for local development (e.g. ipython)
    if [ "${CF_REMOTE_JOB}" != "1" ]; then
        if [ "${CF_REINSTALL_SOFTWARE}" = "1" ]; then
            echo "removing software stack at ${CF_VENV_BASE}"
            rm -rf "${CF_VENV_BASE}"/cf_{prod,dev}
        fi

        show_version_warning() {
            >&2 echo ""
            >&2 echo "WARNING: your venv '$1' is not up to date, please consider updating it in a new shell with"
            >&2 echo "         > CF_REINSTALL_SOFTWARE=1 source setup.sh $( ${setup_is_default} || echo "${setup_name}" )"
            >&2 echo ""
        }

        # source the prod and dev sandboxes
        source "${CF_BASE}/sandboxes/cf_prod.sh" "" "1"
        local ret_prod="$?"
        source "${CF_BASE}/sandboxes/cf_dev.sh" "" "1"
        local ret_dev="$?"

        # show version warnings
        [ "${ret_prod}" = "21" ] && show_version_warning "cf_prod"
        [ "${ret_dev}" = "21" ] && show_version_warning "cf_dev"
    else
        # source the prod sandbox
        source "${CF_BASE}/sandboxes/cf_prod.sh" ""
    fi

    # initialze submodules
    if [ -d "${CF_BASE}/.git" ]; then
        for m in $( ls -1q "${CF_BASE}/modules" ); do
            cf_init_submodule "${CF_BASE}/modules/${m}"
        done
    fi
}

cf_init_submodule() {
    # Initializes and updates a git submodule.
    #
    #
    # Arguments:
    #   1. path
    #       The absolute path to the submodule.

    # local variables
    local path="${1}"

    # do nothing when the path does not exist
    [ ! -d "${path}" ] && return "0"

    # initialize the submodule when the directory is empty
    if [ "$( ls -1q "${path}" | wc -l )" = "0" ]; then
        git submodule update --init --recursive "${path}"
    else
        # update when not on a working branch and there are no changes
        local detached_head="$( ( cd "${path}"; git symbolic-ref -q HEAD &> /dev/null ) && echo true || echo false )"
        local changed_files="$( cd "${path}"; git status --porcelain=v1 2> /dev/null | wc -l )"
        if ! ${detached_head} && [ "${changed_files}" = "0" ]; then
            git submodule update --init --recursive "${path}"
        fi
    fi
}

main() {
    # Invokes the main action of this script, catches possible error codes and prints a message.

    # run the actual setup
    if setup_columnflow "$@"; then
        echo -e "co\x1b[0;49;32ml\x1b[0mumnf\x1b[0;49;32ml\x1b[0mow \x1b[0;49;35msuccessfully setup\x1b[0m"
        return "0"
    else
        local code="$?"
        echo -e "\x1b[0;49;31mcolumnflow setup failed with code ${code}\x1b[0m"
        return "${code}"
    fi
}

# entry point
if [ "${CF_SKIP_SETUP}" != "1" ]; then
    main "$@"
fi
