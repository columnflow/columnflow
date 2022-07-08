Analysis playground
===================

.. image:: https://github.com/uhh-cms/analysis_playground/actions/workflows/lint_and_test.yaml/badge.svg
   :target: https://github.com/uhh-cms/analysis_playground/actions/workflows/lint_and_test.yaml
   :alt: Build status

.. image:: https://codecov.io/gh/uhh-cms/analysis_playground/branch/dev/graph/badge.svg?token=33FLINPXFP
   :target: https://codecov.io/gh/uhh-cms/analysis_playground
   :alt: Code coverge

.. image:: https://readthedocs.org/projects/analysis_playground/badge
   :target: http://analysis_playground.readthedocs.io
   :alt: Documentation status

.. image:: https://img.shields.io/github/license/uhh-cms/analysis_playground.svg
   :target: https://github.com/uhh-cms/analysis_playground/blob/master/LICENSE
   :alt: License

Demonstrator for a Python-based, vectorized analysis with a bunch of public HEP tools.


.. marker-after-header


Quickstart
----------

Modules, exported shell functions and environment variables might have a prefix ``AP`` or ``ap`` to express their connection to this project.

The task that you are about to start requires a valid voms proxy.

.. code-block:: bash

    # clone the project
    git clone --recursive git@github.com:uhh-cms/analysis_playground.git
    cd analysis_playground

    # source the setup and store decisions in .setups/dev.sh (arbitrary name)
    source setup.sh dev

    # index existing tasks once to enable auto-completion for "law run"
    law index --verbose

    # run your first task
    law run ReduceEvents --version v1 --dataset st_tchannel_t --branch 0

    # create plots
    law run PlotVariables \
        --version v1 \
        --datasets "st_tchannel_t,tt_sl" \
        --producers variables \
        --variables ht \
        --workers 3


Development
-----------

- Source hosted at `GitHub <https://github.com/uhh-cms/analysis_playground>`__
- Report issues, questions, feature requests on `GitHub Issues <https://github.com/uhh-cms/analysis_playground/issues>`__


.. marker-after-body
