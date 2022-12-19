.. figure:: https://media.githubusercontent.com/media/uhh-cms/columnflow/master/assets/logo_dark.png
   :width: 480
   :target: https://github.com/uhh-cms/columnflow
   :align: center
   :alt: columnflow logo

.. image:: https://github.com/uhh-cms/columnflow/actions/workflows/lint_and_test.yaml/badge.svg
   :target: https://github.com/uhh-cms/columnflow/actions/workflows/lint_and_test.yaml
   :alt: Build status

.. image:: https://img.shields.io/pypi/v/columnflow.svg?style=flat
   :target: https://pypi.python.org/pypi/columnflow
   :alt: Package version

.. image:: https://readthedocs.org/projects/columnflow/badge/?version=master
   :target: http://columnflow.readthedocs.io
   :alt: Documentation status

.. image:: https://codecov.io/gh/uhh-cms/columnflow/branch/master/graph/badge.svg?token=33FLINPXFP
   :target: https://codecov.io/gh/uhh-cms/columnflow
   :alt: Code coverge

.. image:: https://img.shields.io/github/license/uhh-cms/columnflow.svg
   :target: https://github.com/uhh-cms/columnflow/blob/master/LICENSE
   :alt: License

Backend for vectorized, columnar HEP analyses with pure Python, `law <https://github.com/riga/law>`__ and `order <https://github.com/riga/order>`__.


.. marker-after-header


Note on current development
---------------------------

This project is currently in a beta phase.
The project setup, suggested workflows, definitions of particular tasks, and the signatures of various helper classes and functions are mostly frozen but could still be subject to changes in the near future.
At this point (December 2022), four large-scale analyses based upon columnflow are being developed, and in the process, help test and verify various aspects of its core.
The first released version is expected in early 2023.
However, if you would like to join early on, contribute or just give it a spin, feel free to get in touch!


Quickstart
----------

Modules, exported shell functions and environment variables might have a prefix ``CF`` or ``cf`` to express their connection to this project.

A couple test tasks are listed below.
They might require a **valid voms proxy** for accessing input data.


.. code-block:: bash

    # clone the project
    git clone --recursive git@github.com:uhh-cms/columnflow.git
    cd columnflow

    # source the setup and store decisions in .setups/dev.sh (arbitrary name)
    source setup.sh dev

    # index existing tasks once to enable auto-completion for "law run"
    law index --verbose

    # run your first task
    law run cf.ReduceEvents \
        --version v1 \
        --dataset st_tchannel_t \
        --branch 0

    # create a plot
    # (if "imgcat" is installed for your shell, add ``--view-cmd imgcat``)
    law run cf.PlotVariables1D \
        --version v1 \
        --datasets st_tchannel_t \
        --producers example \
        --variables jet1_pt \
        --categories incl

    # create a (test) datacard (CMS-style)
    law run cf.CreateDatacards \
        --version v1 \
        --producers example \
        --inference-model example \
        --workers 3


Projects using columnflow
-------------------------

- `hh2bbtautau <https://github.com/uhh-cms/hh2bbtautau>`__: HH → bb𝜏𝜏 analysis with CMS.
- `hh2bbww <https://github.com/uhh-cms/hh2bbww>`__: HH → bbWW analysis with CMS.
- `topmass <https://github.com/uhh-cms/topmass>`__: Top quark mass measurement with CMS.
- `mttbar <https://github.com/uhh-cms/mttbar>`__: Search for heavy resonances in ttbar events with CMS.
- `analysis playground <https://github.com/uhh-cms/analysis_playground>`__: A testing playground for HEP analyses.


Development
-----------

- Source hosted at `GitHub <https://github.com/uhh-cms/columnflow>`__
- Report issues, questions, feature requests on `GitHub Issues <https://github.com/uhh-cms/columnflow/issues>`__

.. marker-after-body
