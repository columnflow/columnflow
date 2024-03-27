# Description: Base image for ColumnFlow
# Version: 1.0
# Created by: P. Keicher

# Arguments
# base: Base image. Can either be
#       - a Python image for the most basic setup or
#       - a custom image with the basic software stack for cf
ARG pyversion=""
ARG base="python:$pyversion"
FROM $base

ARG pyversion=""

# executable: Executable to run the container. This should be either the basic
#             setup.sh script or a script to create a specific sandbox.
#             All paths should be relative to the Columnflow base directory.
ARG exe_file
RUN apt-get update
RUN apt-get install curl -y

ENV CF_SOFTWARE_BASE /software

# add python version to global environment
# RUN echo "export PYVERSION=\"$(python -c 'import platform; print(platform.python_version())')\"" >> /root/.bashrc
ENV PYVERSION $pyversion
RUN if [ -z $pyversion ]; then echo "export PYVERSION=\"$(python -c 'import platform; print(platform.python_version())')\"" >> /root/.bashrc ; fi
SHELL ["/bin/bash", "-c"]
RUN echo "Before extra ENV: '$PYVERSION'"
# ENV PYVERSION $PYVERSION
# RUN echo "After extra ENV: '$PYVERSION'"

# setup /afs mount
# RUN --mount type=bind,source="/afs",target=/afs,readonly=true

# ensure that dependencies are built for the current python version
ENV CF_FORCE_COMPILE_ENV "True"

# copy current state of repository into docker image
COPY . /columnflow
ENV CF_BASE /columnflow
WORKDIR ${CF_BASE}
RUN source ./setup.sh && [ "${exe_file}" != "setup.sh" ] && source ${exe_file}
# RUN echo "executing file $exe_file"
# RUN ls -l
# SHELL ["/bin/bash", "-c", "source $exe_file"]
# RUN source "$exe_file"

ENTRYPOINT [ "/bin/bash" ]