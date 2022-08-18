.. figure:: https://media.githubusercontent.com/media/uhh-cms/columnflow/dev/assets/logo_dark.png
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

.. image:: https://codecov.io/gh/uhh-cms/columnflow/branch/dev/graph/badge.svg?token=33FLINPXFP
   :target: https://codecov.io/gh/uhh-cms/columnflow
   :alt: Code coverge

.. image:: https://readthedocs.org/projects/columnflow/badge
   :target: http://columnflow.readthedocs.io
   :alt: Documentation status

.. image:: https://img.shields.io/github/license/uhh-cms/columnflow.svg
   :target: https://github.com/uhh-cms/columnflow/blob/master/LICENSE
   :alt: License

Backend for vectorized, columnar HEP analyses with pure Python, `law <https://github.com/riga/law>`__ and `order <https://github.com/riga/order>`__.


.. marker-after-header


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
    law run cf.PlotVariables \
        --version v1 \
        --datasets st_tchannel_t \
        --producers example \
        --variables jet1_pt \
        --categories 1e \
        --branch 0

    # create a (test) datacard (CMS-style)
    law run cf.CreateDatacards \
        --version v1 \
        --producers example \
        --inference-model example \
        --workers 3


Projects using columnflow
-------------------------

- `analysis playground <https://github.com/uhh-cms/analysis_playground>`__: A testing playground for HEP analyses.
- tba


Development
-----------

- Source hosted at `GitHub <https://github.com/uhh-cms/columnflow>`__
- Report issues, questions, feature requests on `GitHub Issues <https://github.com/uhh-cms/columnflow/issues>`__


.. marker-after-body
