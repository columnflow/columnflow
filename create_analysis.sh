#!/usr/bin/env bash

# Script that creates a minimal analysis project based on columnflow.
#
# Execute as (e.g.)
# > curl https://raw.githubusercontent.com/uhh-cms/columnflow/master/create_analysis.sh | bash
#
# A few variables are queried at the beginning of the project creation and inserted into a template
# analysis. For more insights, checkout the "analysis_template" directory.

create_analysis() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_dir="$( pwd )"


    #
    # helpers
    #

    export_var() {
        local varname="$1"
        local value="$2"

        export $varname="$( eval "echo ${value}" )"
    }

    query_input() {
        local varname="$1"
        local text="$2"
        local default="$3"
        local default_text="${4:-$default}"

        # first query
        local input_line="${text}"
        [ "${default}" != "-" ] && input_line="${input_line} (default: '${default_text}')"
        input_line="${input_line}:  "
        printf "${input_line}"
        read query_response

        # input checks
        while true; do
            # re-query empty values without defaults
            if [ "${default}" = "-" ] && [ "${query_response}" = "" ]; then
                echo "this setting requires a value"
                printf "${input_line}"
                read query_response
                continue
            fi

            # re-query bools in wrong format
            if [ "${default}" = "True" ] || [ "${default}" = "False" ]; then
                if [ "${query_response}" = "" ]; then
                    query_response="${default}"
                elif [ "${query_response,,}" = "true" ]; then
                    query_response="True"
                elif [ "${query_response,,}" = "false" ]; then
                    query_response="False"
                else
                    printf "please enter either True or False:  "
                    read query_response
                    continue
                fi
            fi

            # check characters
            if [[ ! "${query_response}" =~ ^[a-zA-Z0-9_]*$ ]]; then
                echo "only alpha-numeric characters and underscores are allowed"
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

    echo "start creating columnflow-based analysis in local directory"
    echo

    query_input "ca_analysis_name" "Name of the analysis" - "no default"
    query_input "ca_module_name" "Name of the python module in the analysis directory" "${ca_analysis_name,,}"
    query_input "ca_prefix" "Short prefix for environment variables" - "no default"
    query_input "ca_use_ssh" "Use ssh for git submodules" "True"
    echo

    # post-changes
    export ca_prefix="${ca_prefix%_}"
    export ca_prefix_lc="${ca_prefix,,}"
    export ca_prefix_uc="${ca_prefix^^}"


    #
    # checkout the analysis template
    #

    local ca_analysis_base="${this_dir}/${ca_analysis_name}"

    echo "checking out analysis tempate to ${ca_analysis_base}"

    mkdir "${ca_analysis_base}" || return "$?"
    # mkdir "${this_dir}/.cf_analysis_setup" || return "$?"
    # cd "${this_dir}/.cf_analysis_setup"
    # curl -L -s -k https://github.com/uhh-cms/columnflow/tarball/feature/template_analysis | tar -xz || return "$?"
    # mv uhh-cms-columnflow-*/analysis_template/default/* "${ca_analysis_base}" || return "$?"
    # cd "${ca_analysis_base}" || return "$?"
    # rm -rf "${this_dir}/.cf_analysis_setup"

    # dev
    cp -r /afs/desy.de/user/r/riegerma/repos/uhh-cms/hh2bbtautau/modules/columnflow/analysis_template/default/* "${ca_analysis_base}"
    cd "${ca_analysis_base}" || return "$?"
    # dev end

    echo "done"
    echo


    #
    # insert variables
    #

    # rename files
    echo "renaming files"
    find . -depth -name '*__cf_analysis_name__*' -execdir bash -c 'mv -i "$1" "${1//__cf_analysis_name__/'${ca_analysis_name}'}"' bash {} \;
    find . -depth -name '*__cf_module_name__*' -execdir bash -c 'mv -i "$1" "${1//__cf_module_name__/'${ca_module_name}'}"' bash {} \;
    find . -depth -name '*__cf_prefix_lc__*' -execdir bash -c 'mv -i "$1" "${1//__cf_prefix_lc__/'${ca_prefix_lc}'}"' bash {} \;
    find . -depth -name '*__cf_prefix_uc__*' -execdir bash -c 'mv -i "$1" "${1//__cf_prefix_uc__/'${ca_prefix_uc}'}"' bash {} \;
    echo "done"

    echo

    # update files
    echo "inserting placeholders"
    find . -type f -exec sed -i 's/__cf_analysis_name__/'${ca_analysis_name}'/g' {} +
    find . -type f -exec sed -i 's/__cf_module_name__/'${ca_module_name}'/g' {} +
    find . -type f -exec sed -i 's/__cf_prefix_lc__/'${ca_prefix_lc}'/g' {} +
    find . -type f -exec sed -i 's/__cf_prefix_uc__/'${ca_prefix_uc}'/g' {} +
    echo "done"


    #
    # setup git and submodules
    #

    echo
    echo "setup git repository"
    git init -b master
    echo "done"

    echo

    echo "setup submodules"
    mkdir -p modules
    if [ "${ca_use_ssh}" ]; then
        git submodule add git@github.com:uhh-cms/columnflow.git modules/columnflow
        git submodule add git@github.com:uhh-cms/cmsdb.git modules/cmsdb
    else
        git submodule add https://github.com/uhh-cms/columnflow.git modules/columnflow
        git submodule add https://github.com/uhh-cms/cmsdb.git modules/cmsdb
    fi
    git submodule update --init --recursive
    echo "done"


    #
    # minimal setup instructions
    #

    echo
    echo "Setup successfull! The next steps are:"

    echo

    echo "1. Setup the repository and install the environment:"
    echo "  > source setup.sh [optional_setup_name]"

    echo

    echo "2. Checkout the 'Getting started' guide to run your first tasks:"
    echo "  https://columnflow.readthedocs.io/en/master/start.html"

    echo
}

create_analysis "$@"
