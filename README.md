<center>
<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-1-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->
    <a href="https://github.com/uhh-cms/columnflow">
        <img src="https://media.githubusercontent.com/media/uhh-cms/columnflow/master/assets/logo_dark.png" width="480" />
    </a>
</center>

[![Build status](https://github.com/uhh-cms/columnflow/actions/workflows/lint_and_test.yaml/badge.svg)](https://github.com/uhh-cms/columnflow/actions/workflows/lint_and_test.yaml)
[![Package version](https://img.shields.io/pypi/v/columnflow.svg?style=flat)](https://pypi.python.org/pypi/columnflow)
[![Documentation status](https://readthedocs.org/projects/columnflow/badge/?version=master)](http://columnflow.readthedocs.io)
[![Code coverge](https://codecov.io/gh/uhh-cms/columnflow/branch/master/graph/badge.svg?token=33FLINPXFP)](https://codecov.io/gh/uhh-cms/columnflow)
[![License](https://img.shields.io/github/license/uhh-cms/columnflow.svg)](https://github.com/uhh-cms/columnflow/blob/master/LICENSE)

Backend for columnar, fully orchestrated HEP analyses with pure Python, [law](https://github.com/riga/law) and [order](https://github.com/riga/order).

<!-- marker-after-header -->


## Note on current development

This project is currently in a beta phase.
The project setup, suggested workflows, definitions of particular tasks, and the signatures of various helper classes and functions are mostly frozen but could still be subject to changes in the near future.
At this point (December 2022), four large-scale analyses based upon columnflow are being developed, and in the process, help test and verify various aspects of its core.
The first released version is expected in early 2023.
However, if you would like to join early on, contribute or just give it a spin, feel free to get in touch!


## Quickstart

To create an analysis using columnflow, it is recommended to start from a predefined template (located in [analysis_templates](./analysis_templates).
The following command (no previous git clone required) interactively asks for a handful of names and settings, and creates a minimal, yet fully functioning project structure for you!

```shell
    bash -c "$(curl -Ls https://raw.githubusercontent.com/uhh-cms/columnflow/master/create_analysis.sh)"
```

At the end of the setup, you will see further instructions and suggestions to run your first analysis tasks (example below).

```
    Setup successfull! The next steps are:

    1. Setup the repository and install the environment.
      > source setup.sh [optional_setup_name]

    2. Run local tests & linting checks to verify that the analysis is setup correctly.
      > ./tests/run_all

    3. Create a GRID proxy if you intend to run tasks that need one
      > voms-proxy-init -voms cms -rfc -valid 196:00

    4. Checkout the 'Getting started' guide to run your first tasks.
      https://columnflow.readthedocs.io/en/master/start.html

      Suggestions for tasks to run:

      a) Run the 'calibration -> selection -> reduction' pipeline for the first file of the
         default dataset using the default calibrator and default selector
         (enter the command below and 'tab-tab' to see all arguments or add --help for help)
        > law run cf.ReduceEvents --version dev1 --branch 0

      b) Create the jet1_pt distribution for the single top dataset:
        > law run cf.PlotVariables1D --version dev1 --datasets 'st*' --variables jet1_pt

      c) Include the ttbar dataset and also plot jet1_eta:
        > law run cf.PlotVariables1D --version dev1 --datasets 'tt*,st*' --variables jet1_pt,jet1_eta

      d) Create cms-style datacards for the example model in hgg/inference/example.py:
        > law run cf.CreateDatacards --version dev1 --inference-model example
```

For a better overview of the tasks that are triggered by the commands below, checkout the current (yet stylized) [task graph](https://github.com/uhh-cms/columnflow/issues/25#issue-1258137827).


## Projects using columnflow

- [hh2bbtautau](https://github.com/uhh-cms/hh2bbtautau): HH → bb𝜏𝜏 analysis with CMS.
- [hh2bbww](https://github.com/uhh-cms/hh2bbww): HH → bbWW analysis with CMS.
- [topmass](https://github.com/uhh-cms/topmass): Top quark mass measurement with CMS.
- [mttbar](https://github.com/uhh-cms/mttbar): Search for heavy resonances in ttbar events with CMS.
- [analysis playground](https://github.com/uhh-cms/analysis_playground): A testing playground for HEP analyses.


## Contributors

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="http://marcelrieger.com"><img src="https://avatars.githubusercontent.com/u/1908734?v=4?s=100" width="100px;" alt="Marcel R."/><br /><sub><b>Marcel R.</b></sub></a><br /><a href="https://github.com/uhh-cms/columnflow/commits?author=riga" title="Code">💻</a> <a href="https://github.com/uhh-cms/columnflow/commits?author=riga" title="Documentation">📖</a> <a href="https://github.com/uhh-cms/columnflow/commits?author=riga" title="Tests">⚠️</a> <a href="https://github.com/uhh-cms/columnflow/pulls?q=is%3Apr+reviewed-by%3Ariga" title="Reviewed Pull Requests">👀</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->


## Development

- Source hosted at [GitHub](https://github.com/uhh-cms/columnflow)
- Report issues, questions, feature requests on [GitHub Issues](https://github.com/uhh-cms/columnflow/issues)

<!-- marker-after-body -->
