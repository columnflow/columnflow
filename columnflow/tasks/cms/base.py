# coding: utf-8

"""
CMS related base tasks.
"""

from __future__ import annotations

import os

import law

from columnflow.tasks.framework.base import Requirements, AnalysisTask
from columnflow.tasks.framework.remote import (
    RemoteWorkflowMixin, BundleRepo, BundleSoftware, BundleBashSandbox, BundleCMSSWSandbox,
)
from columnflow.util import expand_path


class CrabWorkflow(AnalysisTask, law.cms.CrabWorkflow, RemoteWorkflowMixin):

    # example for a parameter whose value is propagated to the crab job configuration
    crab_memory = law.BytesParameter(
        default=law.NO_FLOAT,
        unit="MB",
        significant=False,
        description="requested memory in MB; empty value leads to crab's default setting; "
        "empty default",
    )

    exclude_params_branch = {"crab_memory"}

    # mapping of environment variables to render variables that are forwarded
    crab_forward_env_variables = {
        "CF_CERN_USER": "cf_cern_user",
        "CF_STORE_NAME": "cf_store_name",
    }

    # upstream requirements
    reqs = Requirements(
        BundleRepo=BundleRepo,
        BundleSoftware=BundleSoftware,
        BundleBashSandbox=BundleBashSandbox,
        BundleCMSSWSandbox=BundleCMSSWSandbox,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cached BundleRepo requirement to avoid race conditions during checksum calculation
        self.bundle_repo_req = self.reqs.BundleRepo.req(self)

    def crab_stageout_location(self) -> tuple[str, str]:
        # the storage site and base directory on it for crab specific outputs
        return (
            law.config.get_expanded("job", "crab_storage_element"),
            law.config.get_expanded("job", "crab_base_directory"),
        )

    def crab_output_directory(self) -> law.FileSystemDirectoryTarget:
        # the directory where submission meta data and logs should be stored
        return self.local_target(dir=True)

    def crab_bootstrap_file(self) -> law.JobInputFile:
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        bootstrap_file = expand_path("$CF_BASE/columnflow/tasks/framework/remote_bootstrap.sh")
        if "CF_REMOTE_BOOTSTRAP_FILE" in os.environ:
            bootstrap_file = os.environ["CF_REMOTE_BOOTSTRAP_FILE"]

        # copy the file only once into the submission directory (sharing between jobs)
        # and allow rendering inside the job
        return law.JobInputFile(bootstrap_file, share=True, render_job=True)

    def crab_workflow_requires(self) -> dict[str, AnalysisTask]:
        reqs = law.cms.CrabWorkflow.crab_workflow_requires(self)

        # add requirements dealing with software bundling
        self.add_bundle_requirements(reqs)

        return reqs

    def crab_job_config(
        self,
        config: law.BaseJobFileFactory.Config,
        submit_jobs: dict[int, list[int]],
    ) -> law.BaseJobFileFactory.Config:
        # add common config settings
        workflow_reqs = self.crab_workflow_requires()
        self.add_common_configs(
            config,
            workflow_reqs,
            law_config=True,
            voms=False,
            kerberos=False,
            wlcg=True,
        )

        # add variables related to software bundles
        self.add_bundle_render_variables(config, workflow_reqs)

        # customize memory
        if self.crab_memory is not None and self.crab_memory > 0:
            config.crab.JobType.maxMemoryMB = int(round(self.crab_memory))

        # render variables
        config.render_variables["cf_bootstrap_name"] = "crab"
        config.render_variables.setdefault("cf_pre_setup_command", "")
        config.render_variables.setdefault("cf_post_setup_command", "")
        config.render_variables.setdefault(
            "cf_remote_lcg_setup",
            law.config.get_expanded("job", "remote_lcg_setup"),
        )

        # forward env variables
        for ev, rv in self.crab_forward_env_variables.items():
            config.render_variables[rv] = os.environ[ev]

        return config

    def crab_destination_info(self, info: dict[str, str]) -> dict[str, str]:
        info = super().crab_destination_info(info)
        info = self.common_destination_info(info)
        return info
