<!-- marker-before-logo -->

<p align="center">
  <img src="https://media.githubusercontent.com/media/columnflow/columnflow/master/assets/logo_dark.png#gh-light-mode-only" width="480" />
  <img src="https://media.githubusercontent.com/media/columnflow/columnflow/master/assets/logo_bright.png#gh-dark-mode-only" width="480" />
</p>

<!-- marker-after-logo -->

<!-- marker-before-badges -->

<p align="center">
  <a href="https://github.com/columnflow/columnflow/actions/workflows/lint_and_test.yaml">
    <img alt="Build status" src="https://github.com/columnflow/columnflow/actions/workflows/lint_and_test.yaml/badge.svg" />
  </a>
  <a href="https://pypi.python.org/pypi/columnflow">
    <img alt="Package version" src="https://img.shields.io/pypi/v/columnflow.svg?style=flat" />
  </a>
  <a href="http://columnflow.readthedocs.io">
    <img alt="Documentation status" src="https://readthedocs.org/projects/columnflow/badge/?version=stable" />
  </a>
  <a href="https://codecov.io/gh/columnflow/columnflow">
    <img alt="Code coverge" src="https://codecov.io/gh/columnflow/columnflow/branch/master/graph/badge.svg?token=33FLINPXFP" />
  </a>
  <a href="https://github.com/columnflow/columnflow/blob/master/LICENSE">
    <img alt="License" src="https://img.shields.io/github/license/columnflow/columnflow.svg" />
  </a>
</p>

<!-- marker-after-badges -->

<!-- marker-before-header -->

Backend for columnar, fully orchestrated HEP analyses with pure Python, [law](https://github.com/riga/law) and [order](https://github.com/riga/order).

This project is for use within the Ghent CMS group. Original source hosted at [GitHub](https://github.com/columnflow/columnflow).

<!-- marker-after-header -->

<!-- marker-before-note -->

<!-- marker-after-note -->

<!-- marker-before-analytics -->

![Alt](https://repobeats.axiom.co/api/embed/8cca127835f18d377e3a691220ae296ac9c80d49.svg "Columnflow Ghent analytics")

<!-- marker-after-analytics -->

<!-- marker-before-body -->

## Quickstart

To create an analysis using columnflow, it is recommended to start from a predefined template (located in [analysis_templates](https://github.com/GhentAnalysis/columnflow/tree/main/analysis_templates)).
The following command (no previous git clone required) interactively asks for a handful of names and settings, and creates a minimal, yet fully functioning project structure for you!
The 'cms_minimal' flavor corresponds to the template provided by columnflow itself. 'Ghent_template' provides a more extensive example.

```shell
bash -c "$(curl -Ls https://raw.githubusercontent.com/GhentAnalysis/columnflow/main/create_analysis.sh)"
```

At the end of the setup, you will see further instructions and suggestions to run your first analysis tasks (example below).

```
Setup successfull! The next steps are:

1. Setup the repository and install the environment.
   > cd
   > source setup.sh [recommended_yet_optional_setup_name]

2. Run local tests & linting checks to verify that the analysis is setup correctly.
   > ./tests/run_all

3. Create a GRID proxy if you intend to run tasks that need one
   > voms-proxy-init -rfc -valid 196:00

4. Checkout the 'Getting started' guide to run your first tasks.
   https://columnflow.readthedocs.io/en/stable/start.html

   Suggestions for tasks to run:

   a) Run the 'calibration -> selection -> reduction' pipeline for the first file (--branch 0) of the
      default dataset using the default calibrator and default selector
      (enter the command below and 'tab-tab' to see all arguments or add --help for help)
      > law run cf.ReduceEvents --version dev1 --branch 0

      Verify what you just run by adding '--print-status -1' (-1 = fully recursive)
      > law run cf.ReduceEvents --version dev1 --branch 0 --print-status -1

   b) Create the jet1_pt distribution for the single top datasets
      (if you have an image/pdf viewer installed, add it via '--view-cmd <binary>')
      > law run cf.PlotVariables1D --version dev1 --datasets 'st*' --variables jet1_pt

      Again, verify what you just ran, now with recursion depth 4
      > law run cf.PlotVariables1D --version dev1 --datasets 'st*' --variables jet1_pt --print-status 4

   c) Include the ttbar dataset and also plot jet1_eta
      > law run cf.PlotVariables1D --version dev1 --datasets 'tt*,st*' --variables jet1_pt,jet1_eta
```

For a better overview of the tasks that are triggered by the commands below, checkout the current (yet stylized) [task graph](https://github.com/columnflow/columnflow/wiki#default-task-graph).


## Other projects and original developers

You can find a list of other projects using columnflow on the [original github](https://github.com/columnflow/columnflow). 
The main contributors to columnflow are also listed there.

## Development

- Source hosted at [GitHub](https://github.com/columnflow/columnflow)
- Report issues, questions, feature requests for columnflow to [GitHub Issues](https://github.com/columnflow/columnflow/issues)

<!-- marker-after-body -->
