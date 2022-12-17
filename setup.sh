#!/usr/bin/env bash

setup_columnflow() {
    # Runs the entire project setup, leading to a collection of environment variables starting with
    # "CF_", the installation of the software stack via virtual environments, and optionally an
    # interactive setup where the user can configure certain variables.
    #
    # Arguments:
    #   1. The name of the setup. "default" (which is itself the default when no name is set)
    #      triggers a setup with good defaults, avoiding all queries to the user and the writing of
    #      a custom setup file. See "interactive_setup()" for more info.
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
    #   CF_REPO_BASE
    #       The base path of the main repository invoking tasks or scripts. Used by columnflow tasks
    #       to identify the repository for e.g. bundling. Set to $CF_BASE when not defined already.
    #   CF_CONDA_BASE
    #       The directory where conda and conda envs are installed. Might point to
    #       $CF_SOFTWARE_BASE/conda.
    #   CF_VENV_BASE
    #       The directory where virtual envs are installed. Might point to $CF_SOFTWARE_BASE/venvs.
    #   CF_CMSSW_BASE
    #       The directory where CMSSW releases are installed. Might point to
    #       $CF_SOFTWARE_BASE/cmssw.
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
    #   CF_PERSISTENT_PATH
    #       PATH fragments that should be considered by sandboxes (bash, venv, cmssw) as having
    #       priority, e.g. to ensure that executable of local packages in used first.
    #   CF_PERSISTENT_PYTHONPATH
    #       PYTHONPATH fragments that should be considered by sandboxes (bash, venv, cmssw) as
    #       having priority, e.g. to ensure that local packages in submodules are imported first.
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
    #   CF_HTCONDOR_FLAVOR
    #       The default htcondor flavor setting for the HTCondorWorkflow task.
    #   CF_SLURM_FLAVOR
    #       The default slurm flavor setting for the SlurmWorkflow task.
    #   CF_SLURM_PARTITION
    #       The default slurm partition setting for the SlurmWorkflow task.
    #   CF_SETUP
    #       A flag that is set to 1 after the setup was successful.
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

    # prevent repeated setups
    if [ "${CF_SETUP}" = "1" ]; then
        >&2 echo "columnflow was already succesfully setup"
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
    # (CF = columnflow)
    #

    # start exporting variables
    export CF_BASE="${this_dir}"
    export CF_SETUP_NAME="${setup_name}"

    # interactive setup
    if [ "${CF_REMOTE_JOB}" != "1" ]; then
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
            query CF_JOB_BASE "Local directory for storing job files" "\$CF_DATA/jobs"
            query CF_VOMS "Virtual-organization" "cms"
            query CF_LOCAL_SCHEDULER "Use a local scheduler for law tasks" "True"
            if [ "${CF_LOCAL_SCHEDULER}" != "True" ]; then
                query CF_SCHEDULER_HOST "Address of a central scheduler for law tasks" "127.0.0.1"
                query CF_SCHEDULER_PORT "Port of a central scheduler for law tasks" "8082"
            else
                export_and_save CF_SCHEDULER_HOST "127.0.0.1"
                export_and_save CF_SCHEDULER_PORT "8082"
            fi
        }
        cf_setup_interactive "${CF_SETUP_NAME}" "${CF_BASE}/.setups/${CF_SETUP_NAME}.sh" || return "$?"
    fi

    # continue the fixed setup
    export CF_REPO_BASE="${CF_REPO_BASE:-$CF_BASE}"
    export CF_CONDA_BASE="${CF_CONDA_BASE:-${CF_SOFTWARE_BASE}/conda}"
    export CF_VENV_BASE="${CF_VENV_BASE:-${CF_SOFTWARE_BASE}/venvs}"
    export CF_CMSSW_BASE="${CF_CMSSW_BASE:-${CF_SOFTWARE_BASE}/cmssw}"
    export CF_CI_JOB="$( [ "${GITHUB_ACTIONS}" = "true" ] && echo 1 || echo 0 )"
    export CF_ORIG_PATH="${PATH}"
    export CF_ORIG_PYTHONPATH="${PYTHONPATH}"
    export CF_ORIG_PYTHON3PATH="${PYTHON3PATH}"
    export CF_ORIG_LD_LIBRARY_PATH="${LD_LIBRARY_PATH}"


    #
    # minimal local software setup
    #

    cf_setup_software_stack "${CF_SETUP_NAME}" || return "$?"


    #
    # common variables
    #

    cf_setup_common_variables || return "$?"


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

    # finalize
    export CF_SETUP="1"
}

cf_setup_common_variables() {
    # Exports variables that might be commonly used across analyses, such as host and job
    # environment variables (or their defaults).

    # lang defaults
    export LANGUAGE="${LANGUAGE:-en_US.UTF-8}"
    export LANG="${LANG:-en_US.UTF-8}"
    export LC_ALL="${LC_ALL:-en_US.UTF-8}"

    # proxy
    export X509_USER_PROXY="${X509_USER_PROXY:-/tmp/x509up_u$( id -u )}"

    # overwrite some variables in remote and ci jobs
    if [ "${CF_REMOTE_JOB}" = "1" ]; then
        export CF_WLCG_USE_CACHE="true"
        export CF_WLCG_CACHE_CLEANUP="true"
        export CF_WORKER_KEEP_ALIVE="false"
    elif [ "${CF_CI_JOB}" = "1" ]; then
        export CF_WORKER_KEEP_ALIVE="false"
    fi

    # luigi worker and scheduler defaults (assigned in law.cfg)
    export CF_WORKER_KEEP_ALIVE="${CF_WORKER_KEEP_ALIVE:-false}"
    export CF_SCHEDULER_HOST="${CF_SCHEDULER_HOST:-127.0.0.1}"
    export CF_SCHEDULER_PORT="${CF_SCHEDULER_PORT:-8082}"

    # default job flavor settings (starting with naf / maxwell cluster defaults)
    # used by law.cfg and, in turn, tasks/framework/remote.py
    local cf_htcondor_flavor_default="naf"
    local cf_slurm_flavor_default="maxwell"
    local cf_slurm_partition_default="cms-uhh"
    local hname="$( hostname 2> /dev/null )"
    if [ "$?" = "0" ]; then
        # lxplus
        if [[ "${hname}" == lx*.cern.ch ]]; then
            cf_htcondor_flavor_default="cern"
        fi
    fi
    export CF_HTCONDOR_FLAVOR="${CF_HTCONDOR_FLAVOR:-${cf_htcondor_flavor_default}}"
    export CF_SLURM_FLAVOR="${CF_SLURM_FLAVOR:-${cf_slurm_flavor_default}}"
    export CF_SLURM_PARTITION="${CF_SLURM_PARTITION:-${cf_slurm_partition_default}}"
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
            echo "using variables for setup '$( cf_color magenta ${setup_name} )' from ${env_file}"
            source "${env_file}" ""
            return "$?"
        elif [ -z "${env_file}" ]; then
            >&2 echo "no env file passed as 2nd argument to cf_interactive_setup"
            return "1"
        else
            echo "no setup file ${env_file} found for setup '$( cf_color magenta ${setup_name} )'"
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
            printf "${text} ($( cf_color default_bright ${varname} ), default $( cf_color default_bright ${default_text} )):  "
            read query_response
            [ "X${query_response}" = "X" ] && query_response="${default}"

            # repeat for boolean flags that were not entered correctly
            while true; do
                ( [ "${default}" != "True" ] && [ "${default}" != "False" ] ) && break
                ( [ "${query_response}" = "True" ] || [ "${query_response}" = "False" ] ) && break
                printf "please enter either '$( cf_color default_bright True )' or '$( cf_color default_bright False )':  " query_response
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

        echo -e "Start querying variables for setup '$( cf_color magenta ${setup_name} )', press enter to accept default values\n"
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
    # Sets up the columnflow software stack consisting of a base environment using conda,
    # lightweight virtual environments on top and git submodule initialization / updates.
    #
    # Arguments:
    #   1. setup_name
    #       The name of the setup.
    #
    # Required environment variables:
    #   CF_BASE
    #       The columnflow base directory.
    #   CF_CONDA_BASE
    #       The directory where conda and conda envs will be installed.
    #   CF_VENV_BASE
    #       The base directory were virtual envs are installed.
    #
    # Optional environments variables:
    #   CF_REMOTE_JOB
    #       When "1", the software stack is sourced but not built.
    #   CF_CI_JOB
    #       When "1", the "cf_prod" venv is skipped and only the "cf_dev" env is built.
    #   CF_REINSTALL_SOFTWARE
    #       When "1", any existing software stack is removed and freshly installed.

    # check global variables
    if [ -z "${CF_BASE}" ]; then
        >&2 echo "CF_BASE not defined, stopping software setup"
        return "1"
    fi
    if [ -z "${CF_CONDA_BASE}" ]; then
        >&2 echo "CF_CONDA_BASE not defined, stopping software setup"
        return "2"
    fi
    if [ -z "${CF_VENV_BASE}" ]; then
        >&2 echo "CF_VENV_BASE not defined, stopping software setup"
        return "3"
    fi

    # local variables
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local setup_name="${1}"
    local setup_is_default="false"
    [ "${setup_name}" = "default" ] && setup_is_default="true"
    local miniconda_source="https://repo.anaconda.com/miniconda/Miniconda3-py39_4.12.0-Linux-x86_64.sh"
    local pyv="3.9"

    # empty the PYTHONPATH
    export PYTHONPATH=""

    # persistent PATH and PYTHONPATH parts that should be
    # priotized over any additions made in sandboxes
    export CF_PERSISTENT_PATH="${CF_BASE}/bin:${CF_BASE}/modules/law/bin:${CF_SOFTWARE_BASE}/bin"
    export CF_PERSISTENT_PYTHONPATH="${CF_BASE}:${CF_BASE}/modules/law:${CF_BASE}/modules/order"

    # prepend them
    export PATH="${CF_PERSISTENT_PATH}:${PATH}"
    export PYTHONPATH="${CF_PERSISTENT_PYTHONPATH}:${PYTHONPATH}"

    # also add the python path of the cenv to be installed to propagate changes to any outer venv
    export PYTHONPATH="${PYTHONPATH}:${CF_CONDA_BASE}/lib/python${pyv}/site-packages"

    # update paths and flags
    export PYTHONWARNINGS="${PYTHONWARNINGS:-ignore}"
    export GLOBUS_THREAD_MODEL="${GLOBUS_THREAD_MODEL:-none}"
    export VIRTUAL_ENV_DISABLE_PROMPT="${VIRTUAL_ENV_DISABLE_PROMPT:-1}"
    export X509_CERT_DIR="${X509_CERT_DIR:-/cvmfs/grid.cern.ch/etc/grid-security/certificates}"
    export X509_VOMS_DIR="${X509_VOMS_DIR:-/cvmfs/grid.cern.ch/etc/grid-security/vomsdir}"
    export X509_VOMSES="${X509_VOMSES:-/cvmfs/grid.cern.ch/etc/grid-security/vomses}"
    export VOMS_USERCONF="${VOMS_USERCONF:-${X509_VOMSES}}"
    ulimit -s unlimited

    # local python stack in one conda env and two virtual envs:
    #   - "cf_prod": contains the minimal stack to run tasks and is sent alongside jobs
    #   - "cf_dev" : "cf_prod" + additional python tools for local development (e.g. ipython)
    if [ "${CF_REMOTE_JOB}" != "1" ]; then
        if [ "${CF_REINSTALL_SOFTWARE}" = "1" ]; then
            echo "removing conda at $( cf_color magenta ${CF_CONDA_BASE})"
            rm -rf "${CF_CONDA_BASE}"

            echo "removing software virtual envs at $( cf_color magenta ${CF_VENV_BASE})"
            rm -rf "${CF_VENV_BASE}"
        fi

        #
        # conda setup
        #

        # not needed in CI jobs
        if [ "${CF_CI_JOB}" != "1" ]; then
            # conda base environment
            local conda_missing="$( [ -d "${CF_CONDA_BASE}" ] && echo "false" || echo "true" )"
            if ${conda_missing}; then
                echo
                cf_color magenta "installing conda at ${CF_CONDA_BASE}"
                (
                    wget "${miniconda_source}" -O setup_miniconda.sh &&
                    bash setup_miniconda.sh -b -u -p "${CF_CONDA_BASE}" &&
                    rm setup_miniconda.sh &&
                    cat << EOF >> "${CF_CONDA_BASE}/.condarc"
changeps1: false
channels:
  - conda-forge
  - defaults
EOF
                )
            fi

            # initialize conda
            source "${CF_CONDA_BASE}/etc/profile.d/conda.sh" "" || return "$?"
            conda activate || return "$?"
            echo "initialized conda with $( cf_color magenta "python ${pyv}" )"

            # install packages
            if ${conda_missing}; then
                echo
                cf_color cyan "setting up conda environment"
                conda install --yes libgcc gfal2 gfal2-util python-gfal2 conda-pack || return "$?"

                # add a file to conda/activate.d that handles the gfal setup transparently with conda-pack
                cat << EOF > "${CF_CONDA_BASE}/etc/conda/activate.d/gfal_activate.sh"
export GFAL_CONFIG_DIR="\${CONDA_PREFIX}/etc/gfal2.d"
export GFAL_PLUGIN_DIR="\${CONDA_PREFIX}/lib/gfal2-plugins"
export X509_CERT_DIR="${X509_CERT_DIR}"
export X509_VOMS_DIR="${X509_VOMS_DIR}"
export X509_VOMSES="${X509_VOMSES}"
export VOMS_USERCONF="${VOMS_USERCONF}"
EOF
            fi
        fi

        #
        # venv setup
        #

        show_version_warning() {
            >&2 echo ""
            >&2 echo "WARNING: your venv '$1' is not up to date, please consider updating it in a new shell with"
            >&2 echo "         > CF_REINSTALL_SOFTWARE=1 source setup.sh $( ${setup_is_default} || echo "${setup_name}" )"
            >&2 echo ""
        }

        # source the prod sandbox, potentially skipped in CI jobs
        if [ "${CF_CI_JOB}" != "1" ]; then
            bash -c "source \"${CF_BASE}/sandboxes/cf_prod.sh\" \"\" \"silent\""
            [ "$?" = "21" ] && show_version_warning "cf_prod"
        fi

        # source the dev sandbox
        source "${CF_BASE}/sandboxes/cf_dev.sh" "" "silent"
        [ "$?" = "21" ] && show_version_warning "cf_dev"

        # initialze submodules
        if [ -e "${CF_BASE}/.git" ]; then
            local m
            for m in $( ls -1q "${CF_BASE}/modules" ); do
                cf_init_submodule "${CF_BASE}" "modules/${m}"
            done
        fi
    else
        # at this point we are located in a remote job

        # initialize conda
        source "${CF_CONDA_BASE}/bin/activate" "" || return "$?"
        echo "initialized conda with $( cf_color magenta "python ${pyv}" )"

        # source the prod sandbox
        source "${CF_BASE}/sandboxes/cf_prod.sh" "" "no"
    fi
}

cf_init_submodule() {
    # Initializes and updates a git submodule.
    #
    # Arguments:
    #   1. base_path
    #     The path of the base directory relative to which the module_path is evaluated.
    #   2. submodule_path
    #       The path to the submodule, relative to base_path.

    # local variables
    local base_path="${1}"
    local submodule_path="${2}"

    # do nothing in remote jobs
    [ "$CF_REMOTE_JOB" = "1" ] && return "0"

    # do nothing when the path does not exist or it is not a submodule
    if [ ! -e "${base_path}/${submodule_path}" ]; then
        return "0"
    fi

    # initialize the submodule when the directory is empty
    if [ "$( ls -1q "${base_path}/${submodule_path}" | wc -l )" = "0" ]; then
        ( cd "${base_path}" && git submodule update --init --recursive "${submodule_path}" )
    else
        # update when not on a working branch and there are no changes
        local detached_head="$( ( cd "${base_path}/${submodule_path}"; git symbolic-ref -q HEAD &> /dev/null ) && echo "true" || echo "false" )"
        local changed_files="$( cd "${base_path}/${submodule_path}"; git status --porcelain=v1 2> /dev/null | wc -l )"
        if ! ${detached_head} && [ "${changed_files}" = "0" ]; then
            ( cd "${base_path}" && git submodule update --init --recursive "${submodule_path}" )
        fi
    fi
}

cf_color() {
    local color="$1"
    local msg="${@:2}"

    # disable coloring in remote jobs
    ( [ "${CF_REMOTE_JOB}" = "1" ] || [ "${CF_CI_JOB}" = "1" ] ) && color="none"

    case "${color}" in
        default)
            echo -e "\x1b[0;49;39m${msg}\x1b[0m"
            ;;
        red)
            echo -e "\x1b[0;49;31m${msg}\x1b[0m"
            ;;
        green)
            echo -e "\x1b[0;49;32m${msg}\x1b[0m"
            ;;
        yellow)
            echo -e "\x1b[0;49;33m${msg}\x1b[0m"
            ;;
        blue)
            echo -e "\x1b[0;49;34m${msg}\x1b[0m"
            ;;
        magenta)
            echo -e "\x1b[0;49;35m${msg}\x1b[0m"
            ;;
        cyan)
            echo -e "\x1b[0;49;36m${msg}\x1b[0m"
            ;;
        default_bright)
            echo -e "\x1b[1;49;39m${msg}\x1b[0m"
            ;;
        red_bright)
            echo -e "\x1b[1;49;31m${msg}\x1b[0m"
            ;;
        green_bright)
            echo -e "\x1b[1;49;32m${msg}\x1b[0m"
            ;;
        yellow_bright)
            echo -e "\x1b[1;49;33m${msg}\x1b[0m"
            ;;
        blue_bright)
            echo -e "\x1b[1;49;34m${msg}\x1b[0m"
            ;;
        magenta_bright)
            echo -e "\x1b[1;49;35m${msg}\x1b[0m"
            ;;
        cyan_bright)
            echo -e "\x1b[1;49;36m${msg}\x1b[0m"
            ;;
        *)
            echo "${msg}"
            ;;
    esac
}

main() {
    # Invokes the main action of this script, catches possible error codes and prints a message.

    # run the actual setup
    if setup_columnflow "$@"; then
        cf_color green "columnflow successfully setup"
        return "0"
    else
        local code="$?"
        cf_color red "columnflow setup failed with code ${code}"
        return "${code}"
    fi
}

# entry point
if [ "${CF_SKIP_SETUP}" != "1" ]; then
    main "$@"
fi
