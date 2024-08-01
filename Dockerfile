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

USER root

ARG pyversion=""
ARG CF_BASE="/columnflow"
ARG CF_SOFTWARE_BASE="/software"

# add python version to global environment
# RUN echo "export PYVERSION=\"$(python -c 'import platform; print(platform.python_version())')\"" >> /root/.bashrc
# copy current state of repository into docker image
COPY . /columnflow
ENV CF_BASE ${CF_BASE}


ENV PYVERSION $pyversion
RUN if [ -z $pyversion ]; then echo "export PYVERSION=\"$(python -c 'import platform; print(platform.python_version())')\"" >> /root/.bashrc ; fi
# executable: Executable to run the container. This should be either the basic
#             setup.sh script or a script to create a specific sandbox.
#             All paths should be relative to the Columnflow base directory.
ARG exe_files
RUN apt-get update
RUN apt-get install curl nano less vim locales git git-lfs -y
RUN locale-gen en_US
RUN locale-gen en_US.UTF-8
RUN update-locale 

# workaround for github action runner
RUN mkdir -p /__w

# create simple user and setup group
RUN getent group cf_user_base || (addgroup --gid 4200 cf_user_base && usermod -a -G cf_user_base root)
RUN getent passwd cf_user || useradd -ms /bin/bash --gid 4200 cf_user

ENV CF_SOFTWARE_BASE /software

# ENV PYVERSION $PYVERSION
# RUN echo "After extra ENV: '$PYVERSION'"
# SHELL ["/columnflow/.docker_entrypoint.sh", "-c"]
SHELL ["/bin/bash", "-c"]


WORKDIR ${CF_BASE}


RUN source ./setup.sh 

RUN source ./setup.sh && for exe_file in ${exe_files//,/ }; do if [ "${exe_file}" != "setup.sh" ]; then bash -c "source ${exe_file}"; fi ; done

# setup ownership so user can also run things
RUN chown cf_user:cf_user_base ${CF_BASE} -R
RUN chown cf_user:cf_user_base ${CF_SOFTWARE_BASE} -R
RUN chown cf_user:cf_user_base /__w -R
USER cf_user

ENTRYPOINT [ "/bin/bash" ]