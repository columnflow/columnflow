# version 1fc3284c59fd57e34598414a92d210730ad3f0eb756e1ade1178bd08758db0d8
# version fa92e45e236a81e1246801e63367290f68de903390e42cc302fd90e6b60ca639
#!/usr/bin/env bash

# Script that sets up a virtual env in $CF_VENV_BASE.
# For more info on functionality and parameters, see the generic setup script _setup_venv.sh.

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # set variables and source the generic venv setup
    export CF_SANDBOX_FILE="${CF_SANDBOX_FILE:-${this_file}}"
    export CF_VENV_NAME="$( basename "${this_file%.sh}" )"
    export CF_VENV_REQUIREMENTS="${CF_REQ_OUTPUT_DIR}/requirements_cf.txt"
    export CF_VENV_ADDITIONAL_REQUIREMENTS=""
    export CF_VENV_EXTRAS=""

    cf_color yellow "Calling '${this_dir}/_setup_venv.sh "$@"'"
    source "${this_dir}/_setup_venv.sh" "$@"
}
action "$@"
