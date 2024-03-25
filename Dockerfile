# Description: Base image for ColumnFlow
# Version: 1.0
# Created by: P. Keicher

# Arguments
# base: Base image. Can either be
#       - a Python image for the most basic setup or
#       - a custom image with the basic software stack for cf
ARG base

# executable: Executable to run the container. This should be either the basic
#             setup.sh script or a script to create a specific sandbox.
#             All paths should be relative to the Columnflow base directory.
ARG executable="setup.sh"

FROM $base

ENV CF_SOFTWARE_BASE /software

# add python version to global environment
RUN echo "export PYVERSION=\"$(python -c 'import platform; print(platform.python_version())')\"" >> /root/.bashrc

# setup /afs mount
RUN --mount type=bind,source="/afs",target=/afs,readonly=true

# ensure that dependencies are built for the current python version
ENV CF_FORCE_COMPILE_ENV "True"

# copy current state of repository into docker immage
COPY . /columnflow
ENV CF_BASE /columnflow
WORKDIR ${CF_BASE}
RUN source $executable