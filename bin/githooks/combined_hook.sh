#!/usr/bin/env bash

# Script that calls all hooks in the same directory with the same name, but ending in a postfix "-*"
# in a sequential way. The script execution fails and stops in case a single hook fails.

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # call all hooks
    local f
    for f in $( ls -1 "${this_dir}/$( basename "${this_file}" )"-* 2> /dev/null || true ); do
        bash "${f}" "$@" || return "$?"
    done

    return "0"
}
action "$@"
