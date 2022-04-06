# Analysis Playground

> Demonstrator for a Python-based, vectorized analysis with a bunch of public HEP tools.

Modules, exported shell functions and environment variables might have a prefix `AP` or `ap` to express their connection to this project.


## Quickstart

The task that you are about to start requires a valid voms proxy!

```bash
# clone the project
git clone --recursive git@github.com:uhh-cms/analysis_playground.git
cd analysis_playground

# source the setup and store decisions in .setups/dev.sh (arbitrary name)
source setup.sh dev

# index existing tasks once to enable auto-completion for "law run"
law index --verbose

# run your first task
law run SelectEvents --version v1 --branch 0
```
