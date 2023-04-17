#! bin/bash

move_if_exists() {
    if ls $1 1> /dev/null 2>&1; then
        mkdir -p $2
        cmd="mv $1 $2"
        echo "$cmd"
        eval "$cmd"
    else
        echo "found no matches for $1, all good"
    fi
}

move_if_exists './conda/' ./data/software
move_if_exists './venv_*/' ./data/software/venvs
move_if_exists './cf_*/' ./data/software/venvs