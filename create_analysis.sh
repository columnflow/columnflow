#!/usr/bin/env bash

# Script that creates a minimal analysis project based on columnflow.
#
# Execute as (e.g.)
# > bash -c "$(curl -Ls https://raw.githubusercontent.com/columnflow/columnflow/master/create_analysis.sh)"
#
# A few variables are queried at the beginning of the project creation and inserted into a template
# analysis. For more insights, checkout the "analysis_templates" directory.

create_analysis() {
    #
    # locals
    #

    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local exec_dir="$( pwd )"
    local fetch_cf_branch="master"
    local fetch_cmsdb_branch="master"
    local debug="false"

    # zsh options
    if ${shell_is_zsh}; then
        setopt globdots
    fi


    #
    # helpers
    #

    export_var() {
        local varname="$1"
        local value="$2"

        export $varname="$( eval "echo ${value}" )"
    }

    echo_color() {
        local color="$1"
        local msg="${@:2}"

        case "${color}" in
            red)
                echo -e "\x1b[0;49;31m${msg}\x1b[0m"
                ;;
            green)
                echo -e "\x1b[0;49;32m${msg}\x1b[0m"
                ;;
            yellow)
                echo -e "\x1b[0;49;33m${msg}\x1b[0m"
                ;;
            cyan)
                echo -e "\x1b[0;49;36m${msg}\x1b[0m"
                ;;
            bright)
                echo -e "\x1b[1;49;39m${msg}\x1b[0m"
                ;;
            green_bright)
                echo -e "\x1b[1;49;32m${msg}\x1b[0m"
                ;;
            *)
                echo "${msg}"
                ;;
        esac
    }

    query_input() {
        local varname="$1"
        local text="$2"
        local default="$3"
        local choices="$4"

        # build the query text
        local input_line="${text}"
        local opened_parenthesis="false"
        if [ ! -z "${choices}" ]; then
            opened_parenthesis="true"
            input_line="${input_line} (choices: '${choices}'"
        fi
        if [ "${default}" != "-" ]; then
            ${opened_parenthesis} && input_line="${input_line}, " || input_line="${input_line} ("
            opened_parenthesis="true"
            input_line="${input_line}default: '${default}'"
        fi
        ${opened_parenthesis} && input_line="${input_line})"
        input_line="${input_line}: "

        # first query
        printf "${input_line}"
        read query_response

        # input checks
        while true; do
            # handle empty responses
            if [ "${query_response}" = "" ]; then
                # re-query empty values without defaults
                if [ "${default}" = "-" ]; then
                    echo_color yellow "a value is required"
                    printf "${input_line}"
                    read query_response
                    continue
                else
                    query_response="${default}"
                fi
            fi

            # compare to choices when given
            if [ ! -z "${choices}" ] && [[ ! ",${choices}," =~ ",${query_response}," ]]; then
                echo_color yellow "invalid choice"
                printf "${input_line}"
                read query_response
                continue
            fi

            # check characters
            if [[ ! "${query_response}" =~ ^[a-zA-Z0-9_]*$ ]]; then
                echo_color yellow "only alpha-numeric characters and underscores are allowed"
                printf "${input_line}"
                read query_response
                continue
            fi

            break
        done

        # strip " and ' on both sides
        query_response="${query_response%\"}"
        query_response="${query_response%\'}"
        query_response="${query_response#\"}"
        query_response="${query_response#\'}"

        export_var "${varname}" "${query_response}"
    }


    #
    # queries
    #

    echo_color bright "start creating columnflow-based analysis in local directory"
    echo

    query_input "cf_analysis_name" "Name of the analysis" "-"
    echo
    query_input "cf_module_name" "Name of the python module in the analysis directory" "${cf_analysis_name,,}"
    echo
    query_input "cf_short_name" "Short name for environment variables, pre- and suffixes" "-"
    echo
    query_input "cf_analysis_flavor" "The flavor of the analysis to setup" "cms_minimal" "cms_minimal"
    echo
    query_input "cf_use_ssh" "Use ssh for git submodules" "True" "True,False"
    echo

    # changes
    export cf_short_name="${cf_short_name%_}"
    export cf_short_name_lc="${cf_short_name,,}"
    export cf_short_name_uc="${cf_short_name^^}"

    # debug output
    if ${debug}; then
        echo "analysis name  : ${cf_analysis_name}"
        echo "module name    : ${cf_module_name}"
        echo "short name lc  : ${cf_short_name_lc}"
        echo "short name uc  : ${cf_short_name_uc}"
        echo "analysis flavor: ${cf_analysis_flavor}"
        echo "use ssh        : ${cf_use_ssh}"
        echo
    fi


    #
    # checkout the analysis template
    #

    local cf_analysis_base="${exec_dir}/${cf_analysis_name}"

    if [ -d "${cf_analysis_base}" ]; then
        >&2 echo "directory '${cf_analysis_base}' already exists, please remove it and start again"
        return "1"
    fi

    echo_color cyan "checking out analysis tempate to ${cf_analysis_base}"

    if ${debug}; then
        cp -r "${this_dir}/analysis_templates/${cf_analysis_flavor}" "${cf_analysis_base}"
        cd "${cf_analysis_base}" || return "$?"
    else
        rm -rf "${exec_dir}/.cf_analysis_setup"
        mkdir -p "${exec_dir}/.cf_analysis_setup" || return "$?"
        cd "${exec_dir}/.cf_analysis_setup"
        curl -L -s -k "https://github.com/columnflow/columnflow/tarball/${fetch_cf_branch}" | tar -xz || return "$?"
        mv columnflow-columnflow-*/"analysis_templates/${cf_analysis_flavor}" "${cf_analysis_base}" || return "$?"
        cd "${cf_analysis_base}" || return "$?"
        rm -rf "${exec_dir}/.cf_analysis_setup"
    fi

    echo_color green "done"
    echo


    #
    # insert variables
    #

    # rename files
    echo_color cyan "renaming files"
    find . -depth -name '*__cf_analysis_name__*' -execdir bash -c 'mv "$1" "${1//__cf_analysis_name__/'${cf_analysis_name}'}"' bash {} \;
    find . -depth -name '*__cf_module_name__*' -execdir bash -c 'mv "$1" "${1//__cf_module_name__/'${cf_module_name}'}"' bash {} \;
    find . -depth -name '*__cf_short_name_lc__*' -execdir bash -c 'mv -i "$1" "${1//__cf_short_name_lc__/'${cf_short_name_lc}'}"' bash {} \;
    find . -depth -name '*__cf_short_name_uc__*' -execdir bash -c 'mv -i "$1" "${1//__cf_short_name_uc__/'${cf_short_name_uc}'}"' bash {} \;
    echo_color green "done"

    echo

    # update files
    echo_color cyan "inserting placeholders"
    find . -type f -execdir sed -i 's/__cf_analysis_name__/'${cf_analysis_name}'/g' {} \;
    find . -type f -execdir sed -i 's/__cf_module_name__/'${cf_module_name}'/g' {} \;
    find . -type f -execdir sed -i 's/__cf_short_name_lc__/'${cf_short_name_lc}'/g' {} \;
    find . -type f -execdir sed -i 's/__cf_short_name_uc__/'${cf_short_name_uc}'/g' {} \;
    echo_color green "done"


    #
    # setup git and submodules
    #

    echo
    echo_color cyan "setup git repository"
    git init
    echo_color green "done"

    echo

    echo_color cyan "enable lfs"
    git lfs install
    echo_color green "done"

    echo

    echo_color cyan "setup submodules"
    mkdir -p modules
    if [ "${cf_use_ssh}" ]; then
        git submodule add -b "${fetch_cf_branch}" git@github.com:columnflow/columnflow.git modules/columnflow
        if [ "${cf_analysis_flavor}" = "cms_minimal" ]; then
            git submodule add -b "${fetch_cmsdb_branch}" git@github.com:uhh-cms/cmsdb.git modules/cmsdb
        fi
    else
        git submodule add -b "${fetch_cf_branch}" https://github.com/columnflow/columnflow.git modules/columnflow
        if [ "${cf_analysis_flavor}" = "cms_minimal" ]; then
            git submodule add "${fetch_cmsdb_branch}" https://github.com/uhh-cms/cmsdb.git modules/cmsdb
        fi
    fi
    git submodule update --init --recursive
    echo_color green "done"


    #
    # minimal setup instructions
    #

    echo
    echo_color green_bright "Setup successfull! The next steps are:"

    echo

    echo_color bright "1. Setup the repository and install the environment."
    echo "  > cd ${cf_analysis_name}"
    echo "  > source setup.sh [optional_setup_name]"

    echo

    echo_color bright "2. Run local tests & linting checks to verify that the analysis is setup correctly."
    echo "  > ./tests/run_all"

    echo

    echo_color bright "3. Create a GRID proxy if you intend to run tasks that need one"
    if [ "${cf_analysis_flavor}" = "cms_minimal" ]; then
        echo "  > voms-proxy-init -voms cms -rfc -valid 196:00"
    else
        echo "  > voms-proxy-init -rfc -valid 196:00"
    fi

    echo

    echo_color bright "4. Checkout the 'Getting started' guide to run your first tasks."
    echo "  https://columnflow.readthedocs.io/en/stable/start.html"

    echo

    echo "  Suggestions for tasks to run:"
    echo

    echo "  a) Run the 'calibration -> selection -> reduction' pipeline for the first file of the"
    echo "     default dataset using the default calibrator and default selector"
    echo "     (enter the command below and 'tab-tab' to see all arguments or add --help for help)"
    echo "    > law run cf.ReduceEvents --version dev1 --branch 0"

    echo

    echo "  b) Create the jet1_pt distribution for the single top dataset:"
    echo "    > law run cf.PlotVariables1D --version dev1 --datasets 'st*' --variables jet1_pt"

    echo

    echo "  c) Include the ttbar dataset and also plot jet1_eta:"
    echo "    > law run cf.PlotVariables1D --version dev1 --datasets 'tt*,st*' --variables jet1_pt,jet1_eta"

    if [ "${cf_analysis_flavor}" = "cms_minimal" ]; then
        echo

        echo "  d) Create cms-style datacards for the example model in ${cf_module_name}/inference/example.py:"
        echo "    > law run cf.CreateDatacards --version dev1 --inference-model example"
    fi

    echo
}

create_analysis "$@"
