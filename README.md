<h1 align="center">
  <img src="https://media.githubusercontent.com/media/columnflow/columnflow/master/assets/logo_dark.png#gh-light-mode-only" width="480" />
  <img src="https://media.githubusercontent.com/media/columnflow/columnflow/master/assets/logo_bright.png#gh-dark-mode-only" width="480" />
</h1>

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

Original source hosted at [GitHub](https://github.com/columnflow/columnflow).

<!-- marker-after-header -->

<!-- marker-before-note -->

## Note on current development

This project is currently in a beta phase.
The project setup, suggested workflows, definitions of particular tasks, and the signatures of various helper classes and functions are mostly frozen but could still be subject to changes in the near future.
At this point (July 2024), various large-scale analyses based upon columnflow are being developed, and in the process, help test and verify various aspects of its core.
The first major release with a largely frozen API is expected in the fall of 2024.
However, if you would like to join early on, contribute or just give it a spin, feel free to get in touch!

<!-- marker-after-note -->

<!-- marker-before-analytics -->

![Columnflow analytics](https://repobeats.axiom.co/api/embed/b6ebc5ba41019de55eb48e195eecb438890442c8.svg "Columnflow analytics")

<!-- marker-after-analytics -->

<!-- marker-before-body -->

## Quickstart

To create an analysis using columnflow, it is recommended to start from a predefined template (located in [analysis_templates](https://github.com/columnflow/columnflow/tree/master/analysis_templates)).
The following command (no previous git clone required) interactively asks for a handful of names and settings, and creates a minimal, yet fully functioning project structure for you!

```shell
bash -c "$(curl -Ls https://raw.githubusercontent.com/columnflow/columnflow/master/create_analysis.sh)"
```

At the end of the setup, you will see further instructions and suggestions to run your first analysis tasks (example below).

```text
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

   a) Run the 'calibration -> selection -> reduction' pipeline for the first file of the
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
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/riga"><img src="https://avatars.githubusercontent.com/u/1908734?v=4?s=100" width="100px;" alt="Marcel Rieger"/><br /><sub><b>Marcel Rieger</b></sub></a><br /><a href="https://github.com/columnflow/columnflow/commits?author=riga" title="Code">💻</a> <a href="https://github.com/columnflow/columnflow/pulls?q=is%3Apr+reviewed-by%3Ariga" title="Reviewed Pull Requests">👀</a> <a href="https://github.com/columnflow/columnflow/commits?author=riga" title="Documentation">📖</a> <a href="https://github.com/columnflow/columnflow/commits?author=riga" title="Tests">⚠️</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/mafrahm"><img src="https://avatars.githubusercontent.com/u/49306645?v=4?s=100" width="100px;" alt="Mathis Frahm"/><br /><sub><b>Mathis Frahm</b></sub></a><br /><a href="https://github.com/columnflow/columnflow/commits?author=mafrahm" title="Code">💻</a> <a href="https://github.com/columnflow/columnflow/pulls?q=is%3Apr+reviewed-by%3Amafrahm" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/dsavoiu"><img src="https://avatars.githubusercontent.com/u/17005255?v=4?s=100" width="100px;" alt="Daniel Savoiu"/><br /><sub><b>Daniel Savoiu</b></sub></a><br /><a href="https://github.com/columnflow/columnflow/commits?author=dsavoiu" title="Code">💻</a> <a href="https://github.com/columnflow/columnflow/pulls?q=is%3Apr+reviewed-by%3Adsavoiu" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/pkausw"><img src="https://avatars.githubusercontent.com/u/26219567?v=4?s=100" width="100px;" alt="pkausw"/><br /><sub><b>pkausw</b></sub></a><br /><a href="https://github.com/columnflow/columnflow/commits?author=pkausw" title="Code">💻</a> <a href="https://github.com/columnflow/columnflow/pulls?q=is%3Apr+reviewed-by%3Apkausw" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/nprouvost"><img src="https://avatars.githubusercontent.com/u/49162277?v=4?s=100" width="100px;" alt="nprouvost"/><br /><sub><b>nprouvost</b></sub></a><br /><a href="https://github.com/columnflow/columnflow/commits?author=nprouvost" title="Code">💻</a> <a href="https://github.com/columnflow/columnflow/commits?author=nprouvost" title="Tests">⚠️</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Bogdan-Wiederspan"><img src="https://avatars.githubusercontent.com/u/79155113?v=4?s=100" width="100px;" alt="Bogdan-Wiederspan"/><br /><sub><b>Bogdan-Wiederspan</b></sub></a><br /><a href="https://github.com/columnflow/columnflow/commits?author=Bogdan-Wiederspan" title="Code">💻</a> <a href="https://github.com/columnflow/columnflow/commits?author=Bogdan-Wiederspan" title="Tests">⚠️</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/kramerto"><img src="https://avatars.githubusercontent.com/u/18616159?v=4?s=100" width="100px;" alt="Tobias Kramer"/><br /><sub><b>Tobias Kramer</b></sub></a><br /><a href="https://github.com/columnflow/columnflow/commits?author=kramerto" title="Code">💻</a> <a href="https://github.com/columnflow/columnflow/pulls?q=is%3Apr+reviewed-by%3Akramerto" title="Reviewed Pull Requests">👀</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/mschrode"><img src="https://avatars.githubusercontent.com/u/5065234?v=4?s=100" width="100px;" alt="Matthias Schroeder"/><br /><sub><b>Matthias Schroeder</b></sub></a><br /><a href="https://github.com/columnflow/columnflow/commits?author=mschrode" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/jolange"><img src="https://avatars.githubusercontent.com/u/6584443?v=4?s=100" width="100px;" alt="Johannes Lange"/><br /><sub><b>Johannes Lange</b></sub></a><br /><a href="https://github.com/columnflow/columnflow/commits?author=jolange" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/BalduinLetzer"><img src="https://avatars.githubusercontent.com/u/70058868?v=4?s=100" width="100px;" alt="BalduinLetzer"/><br /><sub><b>BalduinLetzer</b></sub></a><br /><a href="https://github.com/columnflow/columnflow/commits?author=BalduinLetzer" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/JanekMoels"><img src="https://avatars.githubusercontent.com/u/116348923?v=4?s=100" width="100px;" alt="JanekMoels"/><br /><sub><b>JanekMoels</b></sub></a><br /><a href="#ideas-JanekMoels" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/haddadanas"><img src="https://avatars.githubusercontent.com/u/103462379?v=4?s=100" width="100px;" alt="haddadanas"/><br /><sub><b>haddadanas</b></sub></a><br /><a href="https://github.com/columnflow/columnflow/commits?author=haddadanas" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/jomatthi"><img src="https://avatars.githubusercontent.com/u/82223346?v=4?s=100" width="100px;" alt="jomatthi"/><br /><sub><b>jomatthi</b></sub></a><br /><a href="https://github.com/columnflow/columnflow/commits?author=jomatthi" title="Code">💻</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification.

## Development

- Source hosted at [GitHub](https://github.com/columnflow/columnflow)
- Report issues, questions, feature requests on [GitHub Issues](https://github.com/columnflow/columnflow/issues)

<!-- marker-after-body -->
