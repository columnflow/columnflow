#!/usr/bin/env bash

# get analysisname
echo This script copies the columnflow analysis template
echo and initializes it as a git repository
read -p "Enter the name of the repository: " repository_name
read -p "Enter an abbreveated analyis name (in lower case): " analysis_name
ANALYSIS_NAME=${analysis_name^^}

svn checkout https://github.com/uhh-cms/columnflow/braches/feature/template_analysis/analysis_template $repository_name
cd $repository_name

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

