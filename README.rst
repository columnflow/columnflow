Analysis playground
^^^^^^^^^^^^^^^^^^^

.. image:: https://github.com/uhh-cms/analysis_playground/workflows/Lint%20and%20test/badge.svg
   :target: https://github.com/uhh-cms/analysis_playground/actions?query=workflow%3A%22Lint+and+test%22
   :alt: Build status

.. image:: https://readthedocs.org/projects/analysis_playground/badge/?version=latest
   :target: http://analysis_playground.readthedocs.io/en/latest
   :alt: Documentation status

.. image:: https://img.shields.io/github/license/uhh-cms/analysis_playground.svg
   :target: https://github.com/uhh-cms/analysis_playground/blob/master/LICENSE
   :alt: License

Demonstrator for a Python-based, vectorized analysis with a bunch of public HEP tools.


.. marker-after-header


Quickstart
==========

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
    law run SelectEvents --version v1 --branch 0


Development
===========

- Source hosted at `GitHub <https://github.com/uhh-cms/analysis_playground>`__
- Report issues, questions, feature requests on `GitHub Issues <https://github.com/uhh-cms/analysis_playground/issues>`__


.. marker-after-body
