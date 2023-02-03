#!/usr/bin/env bash

# Script that creates a minimal analysis project based on columnflow.
#
# Execute as (e.g.)
# > curl https://raw.githubusercontent.com/uhh-cms/columnflow/master/create_analysis.sh | bash
#
# A few variables are queried at the beginning of the project creation and inserted into a template
# analysis. For more insights, checkout the "analysis_template" directory.

create_analysis() {
    #
    # locals
    #

    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local exec_dir="$( pwd )"
    local debug="false"
    local fetch_cf_branch="feature/template_analysis"
    local fetch_cmsdb_branch="master"


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
        local choices="$4"

        # build the query text
        local input_line="${text}"
        local opened_parenthesis="false"
        if [ "${default}" != "-" ]; then
            opened_parenthesis="true"
            input_line="${input_line} (default: '${default}'"
        fi
        if [ ! -z "${choices}" ]; then
            ${opened_parenthesis} && input_line="${input_line}, " || input_line="${input_line} ("
            opened_parenthesis="true"
            input_line="${input_line}choices: '${choices}'"
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
                    echo "a value is required"
                    printf "${input_line}"
                    read query_response
                    continue
                else
                    query_response="${default}"
                fi
            fi

            # compare to choices when given
            if [ ! -z "${choices}" ] && [[ ! ",${choices}," =~ ",${query_response}," ]]; then
                echo "invalid choice"
                printf "${input_line}"
                read query_response
                continue
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
    mkdir "${cf_analysis_base}" || return "$?"

    echo "checking out analysis tempate to ${cf_analysis_base}"

    if ${debug}; then
        cp -r "${this_dir}/analysis_template/${cf_analysis_flavor}/"* "${cf_analysis_base}"
        cd "${cf_analysis_base}" || return "$?"
    else
        mkdir "${exec_dir}/.cf_analysis_setup" || return "$?"
        cd "${exec_dir}/.cf_analysis_setup"
        curl -L -s -k "https://github.com/uhh-cms/columnflow/tarball/${fetch_cf_branch}" | tar -xz || return "$?"
        mv uhh-cms-columnflow-*/analysis_template/${cf_analysis_flavor}/* "${cf_analysis_base}" || return "$?"
        cd "${cf_analysis_base}" || return "$?"
        rm -rf "${exec_dir}/.cf_analysis_setup"
    fi

    echo "done"
    echo


    #
    # insert variables
    #

    # rename files
    echo "renaming files"
    find . -depth -name '*__cf_analysis_name__*' -execdir bash -c 'mv "$1" "${1//__cf_analysis_name__/'${cf_analysis_name}'}"' bash {} \;
    find . -depth -name '*__cf_module_name__*' -execdir bash -c 'mv "$1" "${1//__cf_module_name__/'${cf_module_name}'}"' bash {} \;
    find . -depth -name '*__cf_short_name_lc__*' -execdir bash -c 'mv -i "$1" "${1//__cf_short_name_lc__/'${cf_short_name_lc}'}"' bash {} \;
    find . -depth -name '*__cf_short_name_uc__*' -execdir bash -c 'mv -i "$1" "${1//__cf_short_name_uc__/'${cf_short_name_uc}'}"' bash {} \;
    echo "done"

    echo

    # update files
    echo "inserting placeholders"
    find . -type f -execdir sed -i 's/__cf_analysis_name__/'${cf_analysis_name}'/g' {} \;
    find . -type f -execdir sed -i 's/__cf_module_name__/'${cf_module_name}'/g' {} \;
    find . -type f -execdir sed -i 's/__cf_short_name_lc__/'${cf_short_name_lc}'/g' {} \;
    find . -type f -execdir sed -i 's/__cf_short_name_uc__/'${cf_short_name_uc}'/g' {} \;
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
    if [ "${cf_use_ssh}" ]; then
        git submodule add -b "${fetch_cf_branch}" git@github.com:uhh-cms/columnflow.git modules/columnflow
        if [ "${cf_analysis_flavor}" = "cms_minimal" ]; then
            git submodule add -b "${fetch_cmsdb_branch}" git@github.com:uhh-cms/cmsdb.git modules/cmsdb
        fi
    else
        git submodule add -b "${fetch_cf_branch}" https://github.com/uhh-cms/columnflow.git modules/columnflow
        if [ "${cf_analysis_flavor}" = "cms_minimal" ]; then
            git submodule add "${fetch_cmsdb_branch}" https://github.com/uhh-cms/cmsdb.git modules/cmsdb
        fi
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
