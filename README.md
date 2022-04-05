# Analysis Playground

> Demonstrator for a Python-based, vectorized analysis with a bunch of public HEP tools.

Modules, exported shell functions and environment variables might have a prefix `AP` or `ap` to express their connection to this project.


## Quickstart

The task that you are about to start requires a valid voms proxy!

```bash
git clone --recursive git@github.com:uhh-cms/analysis_playground.git
cd analysis_playground

source setup.sh dev

law run SelectEvents --version v1 --branch 0
```
