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

    # post-changes
    ca_prefix="${ca_prefix%_}_"


    #
    # checkout the analysis template
    #

    local ca_analysis_base="${this_dir}/${ca_analysis_name}"
    mkdir "${ca_analysis_base}" || return "$?"
    # mkdir "${this_dir}/.cf_analysis_setup" || return "$?"
    # cd "${this_dir}/.cf_analysis_setup"
    # curl -L -s -k https://github.com/uhh-cms/columnflow/tarball/feature/template_analysis | tar -xz || return "$?"
    # mv uhh-cms-columnflow-*/analysis_template/* "${ca_analysis_base}" || return "$?"
    # cd "${ca_analysis_base}" || return "$?"
    # rm -rf "${this_dir}/.cf_analysis_setup"

    # dev
    cp -r /afs/desy.de/user/r/riegerma/repos/uhh-cms/hh2bbtautau/modules/columnflow/analysis_template/* "${ca_analysis_base}"
    cd "${ca_analysis_base}" || return "$?"
    # dev end


    #
    # setup git and submodules
    #

    git init

    mkdir -p modules
    if [ "${ca_use_ssh}" ]; then
        git submodule add git@github.com:uhh-cms/columnflow.git modules/columnflow
        git submodule add git@github.com:uhh-cms/cmsdb.git modules/cmsdb
    else
        git submodule add https://github.com/uhh-cms/columnflow.git modules/columnflow
        git submodule add https://github.com/uhh-cms/cmsdb.git modules/cmsdb
    fi
    git submodule update --init --recursive


    #
    # replace variables
    #

    # rename files
    find . -depth -name '*plc2hldr*' -execdir bash -c 'mv -i "$1" "${1//plc2hldr/'$repository_name'}"' bash {} \;
    find . -depth -name '*plhld*' -execdir bash -c 'mv -i "$1" "${1//plhld/'$analysis_name'}"' bash {} \;
    find . -depth -name '*PLHLD*' -execdir bash -c 'mv -i "$1" "${1//PLHLD/'$ANALYSIS_NAME'}"' bash {} \;

    # replace variables in files
    find . \( -name .git -o -name modules \) -prune -o -type f -exec sed -i 's/plc2hldr/'$repository_name'/g' {} +
    find . \( -name .git -o -name modules \) -prune -o -type f -exec sed -i 's/plhld/'$analysis_name'/g' {} +
    find . \( -name .git -o -name modules \) -prune -o -type f -exec sed -i 's/PLHLD/'$ANALYSIS_NAME'/g' {} +
}

create_analysis "$@"
