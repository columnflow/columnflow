#!/usr/bin/env bash


check_string(){
    local str=$*
    case $str in
        *" "*)
            read -p "No spaces, please redo: " str
            str=$(check_string $str);;
        "")
            read -p "Empty string, please redo: " str
            str=$(check_string $str);;
        *"!"*|*"@"*|*"#"*|*"$"*|*"%"*|*"^"*|*"&"*|*"*"*|*"("*|*")"*|*"+"*)
            read -p "No special characters, please redo: " str
            str=$(check_string $str);;
    esac
    echo $str
}


# get analysisname
echo This script copies the columnflow analysis template
echo and initializes it as a git repository
read -p "Enter the name of the repository: " repository_name
repository_name=$(check_string $repository_name)
read -p "Enter an abbreveated analyis name (in lower case): " analysis_name
analysis_name=$(check_string $analysis_name)
ANALYSIS_NAME=${analysis_name^^}

svn checkout https://github.com/uhh-cms/columnflow/branches/feature/template_analysis/analysis_template $repository_name

cd $repository_name

rm -rf .svn

git init

# rename directories and files
find . -depth -name '*plc2hldr*' -execdir bash -c 'mv -i "$1" "${1//plc2hldr/'$repository_name'}"' bash {} \;
find . -depth -name '*plhld*' -execdir bash -c 'mv -i "$1" "${1//plhld/'$analysis_name'}"' bash {} \;
find . -depth -name '*PLHLD*' -execdir bash -c 'mv -i "$1" "${1//PLHLD/'$ANALYSIS_NAME'}"' bash {} \;

# replace placeholder inside files
find . \( -name .git -o -name modules \) -prune -o -type f -exec sed -i 's/plc2hldr/'$repository_name'/g' {} +
find . \( -name .git -o -name modules \) -prune -o -type f -exec sed -i 's/plhld/'$analysis_name'/g' {} +
find . \( -name .git -o -name modules \) -prune -o -type f -exec sed -i 's/PLHLD/'$ANALYSIS_NAME'/g' {} +

mkdir modules

git submodule add git@github.com:uhh-cms/columnflow.git modules/columnflow
git submodule add git@github.com:uhh-cms/cmsdb.git modules/cmsdb
git submodule update --init --recursive

git add -A
git commit -m "init"
echo Analysis initialized

