# coding: utf-8

"""
Base classes and tools for working with remote tasks and targets.
"""

import os
import re
import math

import luigi
import law

from columnflow import flavor as cf_flavor
from columnflow.tasks.framework.base import Requirements, AnalysisTask
from columnflow.tasks.framework.parameters import user_parameter_inst
from columnflow.util import real_path


class BundleRepo(AnalysisTask, law.git.BundleGitRepository, law.tasks.TransferLocalFile):

    replicas = luigi.IntParameter(
        default=5,
        description="number of replicas to generate; default: 5",
    )
    user = user_parameter_inst
    version = None

    exclude_files = ["docs", "tests", "data", "assets", ".law", ".setups", ".data", ".github"]

    def get_repo_path(self):
        # required by BundleGitRepository
        return os.environ["CF_REPO_BASE"]

    def single_output(self):
        repo_base = os.path.basename(self.get_repo_path())
        return self.target(f"{repo_base}.{self.checksum}.tgz")

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else r"[^\.]+")

    def output(self):
        return law.tasks.TransferLocalFile.output(self)

    @law.decorator.log
    @law.decorator.safe_output
    def run(self):
        # create the bundle
        bundle = law.LocalFileTarget(is_tmp="tgz")
        self.bundle(bundle)

        # log the size
        self.publish_message(f"size is {law.util.human_bytes(bundle.stat().st_size, fmt=True)}")

        # transfer the bundle
        self.transfer(bundle)


class BundleSoftware(AnalysisTask, law.tasks.TransferLocalFile):

    replicas = luigi.IntParameter(
        default=5,
        description="number of replicas to generate; default: 5",
    )
    user = user_parameter_inst
    version = None

    def single_output(self):
        return self.target("software.tgz")

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else r"[^\.]+")

    @law.decorator.log
    @law.decorator.safe_output
    def run(self):
        software_path = os.environ["CF_CONDA_BASE"]

        # create the local bundle
        bundle = law.LocalFileTarget(is_tmp=".tgz")

        # create the archive with a custom filter
        with self.publish_step("bundling software stack ..."):
            cmd = f"conda-pack --prefix {software_path} --output {bundle.path}"
            code = law.util.interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
        if code != 0:
            raise Exception("conda-pack failed")

        # log the size
        size, unit = law.util.human_bytes(bundle.stat().st_size)
        self.publish_message(f"size is {size:.2f} {unit}")

        # transfer the bundle
        self.transfer(bundle)


class SandboxFileTask(AnalysisTask):

    sandbox_file = luigi.Parameter(
        description="the sandbox file to install",
    )
    user = user_parameter_inst

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        # update the sandbox file when set
        if params.get("sandbox_file") not in (None, "", law.NO_STR):
            # expand variables
            path = os.path.expandvars(os.path.expanduser(params["sandbox_file"]))
            # remove optional sandbox types
            path = law.Sandbox.remove_type(path)
            # add default file extension
            if not os.path.splitext(path)[1]:
                path += ".sh"
            # save again
            params["sandbox_file"] = path

        return params


class BuildBashSandbox(SandboxFileTask):

    sandbox = luigi.Parameter(
        default=law.NO_STR,
        description="do not set manually",
    )

    version = None

    exclude_params_index = {"sandbox"}

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        # resolve the sandbox file relative to $CF_BASE/sandboxes
        if "sandbox_file" in params:
            path = params["sandbox_file"]
            abs_path = real_path(path)
            path = abs_path if os.path.exists(abs_path) else os.path.join("$CF_BASE", "sandboxes", path)
            params["sandbox_file"] = path
            params["sandbox"] = law.Sandbox.join_key("bash", path)

        return params

    def output(self):
        # note: invoking self.env will already trigger installing the sandbox
        return law.LocalFileTarget(self.env["CF_SANDBOX_FLAG_FILE"])

    def run(self):
        # no need to run anything as the sandboxing mechanism handles the installation
        return


class BundleBashSandbox(AnalysisTask, law.tasks.TransferLocalFile):

    sandbox_file = luigi.Parameter(
        description="name of the bash sandbox file; when not absolute, the path is evaluated "
        "relative to $CF_BASE/sandboxes",
    )
    replicas = luigi.IntParameter(
        default=5,
        description="number of replicas to generate; default: 5",
    )
    user = user_parameter_inst
    version = None

    # upstream requirements
    reqs = Requirements(
        BuildBashSandbox=BuildBashSandbox,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # get the name and install path of the sandbox
        from cf_sandbox_file_hash import create_sandbox_file_hash
        self.sandbox_file_hash = create_sandbox_file_hash(self.sandbox_file)
        self.venv_name = os.path.splitext(os.path.basename(self.sandbox_file))[0]
        self.venv_name_hashed = f"{self.venv_name}_{self.sandbox_file_hash}"
        self.venv_path = os.path.join(os.environ["CF_VENV_BASE"], self.venv_name_hashed)

        # checksum cache
        self._checksum = None

    def requires(self):
        return self.reqs.BuildBashSandbox.req(self)

    @property
    def checksum(self):
        if not self._checksum:
            # get the flag file
            flag_file = os.path.join(self.venv_path, "cf_flag")

            # hash the content
            if os.path.isfile(flag_file):
                with open(flag_file, "r") as f:
                    content = (flag_file, f.read().strip())
                self._checksum = law.util.create_hash(content)

        return self._checksum

    def single_output(self):
        checksum = self.checksum or "TO_BE_INSTALLED"
        return self.target(f"{self.venv_name_hashed}.{checksum}.tgz")

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else r"[^\.]+")

    @law.decorator.log
    @law.decorator.safe_output
    def run(self):
        # create the local bundle
        bundle = law.LocalFileTarget(is_tmp=".tgz")

        def _filter(tarinfo):
            # skip hidden dev files
            if re.search(r"(\.pyc|\/\.git|\.tgz|__pycache__)$", tarinfo.name):
                return None
            return tarinfo

        # create the archive with a custom filter
        with self.publish_step(f"bundling venv {self.venv_name} ..."):
            bundle.dump(self.venv_path, add_kwargs={"filter": _filter}, formatter="tar")

        # log the size
        self.publish_message(f"size is {law.util.human_bytes(bundle.stat().st_size, fmt=True)}")

        # transfer the bundle
        self.transfer(bundle)


class BundleCMSSWSandbox(SandboxFileTask, law.cms.BundleCMSSW, law.tasks.TransferLocalFile):

    sandbox_file = luigi.Parameter(
        description="name of the cmssw sandbox file; when not absolute, the path is evaluated "
        "relative to $CF_BASE/sandboxes",
    )
    replicas = luigi.IntParameter(
        default=5,
        description="number of replicas to generate; default: 5",
    )
    version = None

    exclude = "^src/tmp"
    include = ("venv", "venvs")

    # upstream requirements
    reqs = Requirements(
        BuildBashSandbox=BuildBashSandbox,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # get the name and install path of the sandbox
        from cf_sandbox_file_hash import create_sandbox_file_hash
        self.sandbox_file_hash = create_sandbox_file_hash(self.sandbox_file)
        self.cmssw_env_name = os.path.splitext(os.path.basename(self.sandbox_file))[0]
        self.cmssw_env_name_hashed = f"{self.cmssw_env_name}_{self.sandbox_file_hash}"

    def requires(self):
        return self.reqs.BuildBashSandbox.req(self)

    def get_cmssw_path(self):
        # invoking .env will already trigger building the sandbox
        req = self.requires()
        if getattr(req, "sandbox_inst", None):
            return req.sandbox_inst.env["CMSSW_BASE"]
        if "CMSSW_BASE" in os.environ:
            return os.environ["CMSSW_BASE"]
        raise Exception("could not determine CMSSW_BASE")

    def single_output(self):
        cmssw_path = os.path.basename(self.get_cmssw_path())
        return self.target(f"{self.cmssw_env_name_hashed}_{cmssw_path}.{self.checksum}.tgz")

    def output(self):
        return law.tasks.TransferLocalFile.output(self)

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else r"[^\.]+")

    @law.decorator.log
    def run(self):
        # create the bundle
        bundle = law.LocalFileTarget(is_tmp="tgz")
        self.bundle(bundle)

        # log the size
        self.publish_message(f"size is {law.util.human_bytes(bundle.stat().st_size, fmt=True)}")

        # transfer the bundle and mark the task as complete
        self.transfer(bundle)


class RemoteWorkflowMixin(object):
    """
    Mixin class for custom remote workflows adding common functionality.
    """

    skip_destination_info: bool = False

    def add_bundle_requirements(
        self,
        reqs: dict[str, AnalysisTask],
        *,
        share_software: bool,
    ) -> None:
        """
        Adds requirements related to bundles of the repository, conda environment, bash and cmssw
        sandboxes to *reqs*.

        :param reqs: Dictionary of workflow requirements to be extended.
        """
        # add the repository bundle and trigger the checksum calculation
        if getattr(self, "bundle_repo_req", None) is not None:
            reqs["repo_bundle"] = self.bundle_repo_req
        elif "BundleRepo" in self.reqs:
            reqs["repo_bundle"] = self.reqs.BundleRepo.req(self)
        if "repo_bundle" in reqs:
            self.bundle_repo_req.checksum

        # main software stack
        if not share_software and "BundleSoftware" in self.reqs:
            reqs["software_bundle"] = self.reqs.BundleSoftware.req(self)

        # get names of bash and cmssw sandboxes
        bash_sandboxes = set()
        cmssw_sandboxes = set()
        if getattr(self, "analysis_inst", None) is not None:
            bash_sandboxes |= set(self.analysis_inst.x("bash_sandboxes", []))
            cmssw_sandboxes |= set(self.analysis_inst.x("cmssw_sandboxes", []))
        if getattr(self, "config_inst", None) is not None:
            bash_sandboxes |= set(self.config_inst.x("bash_sandboxes", []))
            cmssw_sandboxes |= set(self.config_inst.x("cmssw_sandboxes", []))

        # remove leading sandbox types
        bash_sandboxes = {law.Sandbox.remove_type(s) for s in bash_sandboxes}
        cmssw_sandboxes = {law.Sandbox.remove_type(s) for s in cmssw_sandboxes}

        # build sandboxes when sharing software, otherwise require bundles
        if share_software:
            if "BuildBashSandbox" in self.reqs:
                if bash_sandboxes:
                    reqs["bash_sandbox_builds"] = [
                        self.reqs.BuildBashSandbox.req(self, sandbox_file=sandbox_file)
                        for sandbox_file in sorted(bash_sandboxes)
                    ]
                if cmssw_sandboxes:
                    reqs["cmssw_sandbox_builds"] = [
                        self.reqs.BuildBashSandbox.req(self, sandbox_file=sandbox_file)
                        for sandbox_file in sorted(cmssw_sandboxes)
                    ]
        else:
            if "BundleBashSandbox" in self.reqs and bash_sandboxes:
                reqs["bash_sandbox_bundles"] = [
                    self.reqs.BundleBashSandbox.req(self, sandbox_file=sandbox_file)
                    for sandbox_file in sorted(bash_sandboxes)
                ]
            if "BundleCMSSWSandbox" in self.reqs and cmssw_sandboxes:
                reqs["cmssw_sandbox_bundles"] = [
                    self.reqs.BundleCMSSWSandbox.req(self, sandbox_file=sandbox_file)
                    for sandbox_file in sorted(cmssw_sandboxes)
                ]

    def add_bundle_render_variables(
        self,
        config: law.BaseJobFileFactory.Config,
        reqs: dict[str, AnalysisTask],
    ) -> None:
        """
        Adds render variables to the job *config* related to repository, conda environment, bash and
        cmssw sandboxes, depending on which requirements are present in *reqs*.

        :param reqs: Dictionary of workflow requirements.
        :param config: The job :py:class:`law.BaseJobFileFactory.Config` whose render variables
            should be set.
        """
        join_bash = lambda seq: " ".join(map('"{}"'.format, seq))

        def get_bundle_info(task):
            uris = task.output().dir.uri(base_name="filecopy", return_all=True)
            pattern = os.path.basename(task.get_file_pattern())
            return ",".join(uris), pattern

        # add repo variables
        if "repo_bundle" in reqs:
            uris, pattern = get_bundle_info(reqs["repo_bundle"])
            config.render_variables["cf_repo_uris"] = uris
            config.render_variables["cf_repo_pattern"] = pattern

        # add software variables
        if "software_bundle" in reqs:
            uris, pattern = get_bundle_info(reqs["software_bundle"])
            config.render_variables["cf_software_uris"] = uris
            config.render_variables["cf_software_pattern"] = pattern

        # add bash sandbox variables
        if "bash_sandbox_bundles" in reqs:
            uris, patterns = law.util.unzip([get_bundle_info(t) for t in reqs["bash_sandbox_bundles"]])
            names = [
                os.path.splitext(os.path.basename(t.sandbox_file))[0]
                for t in reqs["bash_sandbox_bundles"]
            ]
            config.render_variables["cf_bash_sandbox_uris"] = join_bash(uris)
            config.render_variables["cf_bash_sandbox_patterns"] = join_bash(patterns)
            config.render_variables["cf_bash_sandbox_names"] = join_bash(names)

        # add cmssw sandbox variables
        if "cmssw_sandbox_bundles" in reqs:
            uris, patterns = law.util.unzip([get_bundle_info(t) for t in reqs["cmssw_sandbox_bundles"]])
            names = [
                os.path.splitext(os.path.basename(t.sandbox_file))[0]
                for t in reqs["cmssw_sandbox_bundles"]
            ]
            config.render_variables["cf_cmssw_sandbox_uris"] = join_bash(uris)
            config.render_variables["cf_cmssw_sandbox_patterns"] = join_bash(patterns)
            config.render_variables["cf_cmssw_sandbox_names"] = join_bash(names)

    def add_common_configs(
        self,
        config: law.BaseJobFileFactory.Config,
        reqs: dict[str, AnalysisTask],
        *,
        law_config: bool = True,
        voms: bool = True,
        kerberos: bool = False,
        wlcg: bool = True,
    ) -> None:
        """
        Adds job settings like common input files or render variables to the job *config*. Workflow
        requirements are given as *reqs* to let common options potentially depend on them.
        Additional keyword arguments control specific behavior of this method.

        :param reqs: Dictionary of workflow requirements.
        :param config: The job :py:class:`law.BaseJobFileFactory.Config`.
        :param law_config: Whether the law config should be forwarded (via render variables or input
            file).
        :param voms: Whether the voms proxy file should be forwarded.
        :param kerberos: Whether the kerberos proxy file should be forwarded.
        :param wlcg: Whether WLCG specific settings should be added.
        """
        # when the law config file is located within CF_REPO_BASE, just set a render variable,
        # but otherwise send it as an input file
        if law_config:
            rel_path = os.path.relpath(os.environ["LAW_CONFIG_FILE"], os.environ["CF_REPO_BASE"])
            if not rel_path.startswith(".."):
                config.render_variables["law_config_file"] = os.path.join("$CF_REPO_BASE", rel_path)
            else:
                config.input_files["law_config_file"] = law.JobInputFile(
                    "$LAW_CONFIG_FILE",
                    share=True,
                    render=False,
                )

        # forward voms proxy
        if voms and not law.config.get_expanded_boolean("analysis", "skip_ensure_proxy", False):
            vomsproxy_file = law.wlcg.get_vomsproxy_file()
            if not law.wlcg.check_vomsproxy_validity(proxy_file=vomsproxy_file):
                raise Exception("voms proxy not valid, submission aborted")
            config.input_files["vomsproxy_file"] = law.JobInputFile(
                vomsproxy_file,
                share=True,
                render=False,
            )

        # forward kerberos proxy
        if kerberos and "KRB5CCNAME" in os.environ:
            kfile = os.environ["KRB5CCNAME"]
            kerberos_proxy_file = os.sep + kfile.split(os.sep, 1)[-1]
            if os.path.exists(kerberos_proxy_file):
                config.input_files["kerberosproxy_file"] = law.JobInputFile(
                    kerberos_proxy_file,
                    share=True,
                    render=False,
                )

                # set the pre command to extend potential afs permissions
                if not config.render_variables.get("cf_pre_setup_command"):
                    config.render_variables["cf_pre_setup_command"] = "aklog"

        # add the wlcg tools
        if wlcg:
            config.input_files["wlcg_tools"] = law.JobInputFile(
                law.util.law_src_path("contrib/wlcg/scripts/law_wlcg_tools.sh"),
                share=True,
                render=False,
            )

    def common_destination_info(self, info: dict[str, str]) -> dict[str, str]:
        """
        Hook to modify the additional info printed along logs of the workflow.
        """
        if self.skip_destination_info:
            return info

        if getattr(self, "config_inst", None) is not None:
            info["config"] = self.config_inst.name
        if getattr(self, "dataset_inst", None) is not None:
            info["dataset"] = self.dataset_inst.name
        if getattr(self, "global_shift_inst", None) not in (None, law.NO_STR, "nominal"):
            info["shift"] = self.global_shift_inst.name

        return info


_default_htcondor_flavor = law.config.get_expanded("analysis", "htcondor_flavor", law.NO_STR)
_default_htcondor_share_software = law.config.get_expanded_boolean("analysis", "htcondor_share_software", False)


class HTCondorWorkflow(AnalysisTask, law.htcondor.HTCondorWorkflow, RemoteWorkflowMixin):

    transfer_logs = luigi.BoolParameter(
        default=True,
        significant=False,
        description="transfer job logs to the output directory; default: True",
    )
    max_runtime = law.DurationParameter(
        default=2.0,
        unit="h",
        significant=False,
        description="maximum runtime; default unit is hours; default: 2",
    )
    htcondor_logs = luigi.BoolParameter(
        default=False,
        significant=False,
        description="transfer htcondor internal submission logs to the output directory; "
        "default: False",
    )
    htcondor_cpus = luigi.IntParameter(
        default=law.NO_INT,
        significant=False,
        description="number of CPUs to request; empty value leads to the cluster default setting; "
        "empty default",
    )
    htcondor_gpus = luigi.IntParameter(
        default=law.NO_INT,
        significant=False,
        description="number of GPUs to request; empty value leads to the cluster default setting; "
        "empty default",
    )
    htcondor_memory = law.BytesParameter(
        default=law.NO_FLOAT,
        unit="MB",
        significant=False,
        description="requested memeory in MB; empty value leads to the cluster default setting; "
        "empty default",
    )
    htcondor_flavor = luigi.ChoiceParameter(
        default=_default_htcondor_flavor,
        choices=(
            "naf", "naf_el7", "naf_el9", "cern", "cern_el7", "cern_el8", "cern_el9", law.NO_STR,
        ),
        significant=False,
        description="the 'flavor' (i.e. configuration name) of the batch system; choices: "
        "naf,naf_el7,naf_el9,cern,cern_el7,cern_el8,cern_el9,NO_STR; "
        f"default: '{_default_htcondor_flavor}'",
    )
    htcondor_share_software = luigi.BoolParameter(
        default=_default_htcondor_share_software,
        significant=False,
        description="when True, do not bundle and download software plus sandboxes but instruct "
        "jobs to use the software in the current CF_SOFTWARE_BASE if accessible; default: "
        f"{_default_htcondor_share_software}",
    )

    exclude_params_branch = {
        "max_runtime", "htcondor_logs", "htcondor_cpus", "htcondor_gpus", "htcondor_memory",
        "htcondor_flavor", "htcondor_share_software",
    }

    # mapping of environment variables to render variables that are forwarded
    htcondor_forward_env_variables = {
        "CF_BASE": "cf_base",
        "CF_REPO_BASE": "cf_repo_base",
        "CF_CERN_USER": "cf_cern_user",
        "CF_STORE_NAME": "cf_store_name",
        "CF_STORE_LOCAL": "cf_store_local",
        "CF_LOCAL_SCHEDULER": "cf_local_scheduler",
    }

    # upstream requirements
    reqs = Requirements(
        BundleRepo=BundleRepo,
        BundleSoftware=BundleSoftware,
        BuildBashSandbox=BuildBashSandbox,
        BundleBashSandbox=BundleBashSandbox,
        BundleCMSSWSandbox=BundleCMSSWSandbox,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cached BundleRepo requirement to avoid race conditions during checksum calculation
        self.bundle_repo_req = self.reqs.BundleRepo.req(self)

    def htcondor_workflow_requires(self):
        reqs = law.htcondor.HTCondorWorkflow.htcondor_workflow_requires(self)

        # add requirements dealing with sandbox/software building and bundling
        self.add_bundle_requirements(reqs, share_software=self.htcondor_share_software)

        return reqs

    def htcondor_output_directory(self):
        # the directory where submission meta data and logs should be stored
        return self.local_target(dir=True)

    def htcondor_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        bootstrap_file = os.path.expandvars("$CF_BASE/columnflow/tasks/framework/remote_bootstrap.sh")
        if "CF_REMOTE_BOOTSTRAP_FILE" in os.environ:
            bootstrap_file = os.environ["CF_REMOTE_BOOTSTRAP_FILE"]

        # copy the file only once into the submission directory (sharing between jobs)
        # and allow rendering inside the job
        return law.JobInputFile(bootstrap_file, share=True, render_job=True)

    def htcondor_job_config(self, config, job_num, branches):
        # add common config settings
        workflow_reqs = self.htcondor_workflow_requires()
        self.add_common_configs(
            config,
            workflow_reqs,
            law_config=True,
            voms=True,
            kerberos=False,
            wlcg=True,
        )

        # add variables related to software bundles
        self.add_bundle_render_variables(config, workflow_reqs)

        # some htcondor setups require a "log" config, but we can safely use /dev/null by default
        config.log = "log.txt" if self.htcondor_logs else "/dev/null"

        # default lcg setup file
        remote_lcg_setup = law.config.get_expanded("job", "remote_lcg_setup_el9")

        # CERN settings, https://batchdocs.web.cern.ch/local/submit.html#os-selection-via-containers
        if self.htcondor_flavor.startswith("cern"):
            cern_os = "el9"
            if self.htcondor_flavor == "cern_el7":
                cern_os = "el7"
                remote_lcg_setup = law.config.get_expanded("job", "remote_lcg_setup_el7")
            elif self.htcondor_flavor == "cern_el8":
                cern_os = "el8"
            config.custom_content.append(("MY.WantOS", cern_os))

        # NAF settings (https://confluence.desy.de/display/IS/BIRD)
        if self.htcondor_flavor.startswith("naf"):
            if self.htcondor_flavor == "naf_el7":
                config.custom_content.append(("requirements", "(OpSysAndVer == \"CentOS7\")"))
            else:
                config.custom_content.append(("Request_OpSysAndVer", "\"RedHat9\""))

        # maximum runtime, compatible with multiple batch systems
        if self.max_runtime is not None and self.max_runtime > 0:
            max_runtime = int(math.floor(self.max_runtime * 3600)) - 1
            config.custom_content.append(("+MaxRuntime", max_runtime))
            config.custom_content.append(("+RequestRuntime", max_runtime))

        # request cpus
        if self.htcondor_cpus is not None and self.htcondor_cpus > 0:
            config.custom_content.append(("RequestCpus", self.htcondor_cpus))

        # request gpus
        if self.htcondor_gpus is not None and self.htcondor_gpus > 0:
            # TODO: the exact setting might be flavor dependent in the future
            # e.g. https://confluence.desy.de/display/IS/GPU+on+NAF
            config.custom_content.append(("Request_GPUs", self.htcondor_gpus))

        # request memory
        if self.htcondor_memory is not None and self.htcondor_memory > 0:
            config.custom_content.append(("Request_Memory", self.htcondor_memory))

        # render variables
        config.render_variables["cf_bootstrap_name"] = "htcondor_standalone"
        if self.htcondor_flavor not in ("", law.NO_STR):
            config.render_variables["cf_htcondor_flavor"] = self.htcondor_flavor
        config.render_variables.setdefault("cf_pre_setup_command", "")
        config.render_variables.setdefault("cf_post_setup_command", "")
        config.render_variables.setdefault("cf_remote_lcg_setup", remote_lcg_setup)
        if self.htcondor_share_software:
            config.render_variables["cf_software_base"] = os.environ["CF_SOFTWARE_BASE"]

        # forward env variables
        for ev, rv in self.htcondor_forward_env_variables.items():
            config.render_variables[rv] = os.environ[ev]

        return config

    def htcondor_use_local_scheduler(self):
        # remote jobs should not communicate with ther central scheduler but with a local one
        return True

    def htcondor_destination_info(self, info: dict[str, str]) -> dict[str, str]:
        info = super().htcondor_destination_info(info)
        info = self.common_destination_info(info)
        return info


_default_slurm_flavor = law.config.get_expanded("analysis", "slurm_flavor", "maxwell")
_default_slurm_partition = law.config.get_expanded("analysis", "slurm_partition", "cms-uhh")


class SlurmWorkflow(AnalysisTask, law.slurm.SlurmWorkflow, RemoteWorkflowMixin):

    transfer_logs = luigi.BoolParameter(
        default=True,
        significant=False,
        description="transfer job logs to the output directory; default: True",
    )
    max_runtime = law.DurationParameter(
        default=2.0,
        unit="h",
        significant=False,
        description="maximum runtime; default unit is hours; default: 2",
    )
    slurm_partition = luigi.Parameter(
        default=_default_slurm_partition,
        significant=False,
        description=f"target queue partition; default: {_default_slurm_partition}",
    )
    slurm_flavor = luigi.ChoiceParameter(
        default=_default_slurm_flavor,
        choices=("maxwell",),
        significant=False,
        description="the 'flavor' (i.e. configuration name) of the batch system; choices: "
        f"maxwell; default: '{_default_slurm_flavor}'",
    )

    exclude_params_branch = {"max_runtime", "slurm_partition", "slurm_flavor"}

    # mapping of environment variables to render variables that are forwarded
    slurm_forward_env_variables = {
        "CF_BASE": "cf_base",
        "CF_REPO_BASE": "cf_repo_base",
        "CF_CERN_USER": "cf_cern_user",
        "CF_STORE_NAME": "cf_store_name",
        "CF_STORE_LOCAL": "cf_store_local",
        "CF_LOCAL_SCHEDULER": "cf_local_scheduler",
    }

    # upstream requirements
    reqs = Requirements(
        BuildBashSandbox=BuildBashSandbox,
    )

    def slurm_workflow_requires(self):
        reqs = law.slurm.SlurmWorkflow.slurm_workflow_requires(self)

        # add requirements dealing with sandbox/software building and bundling
        self.add_bundle_requirements(reqs, share_software=True)

        return reqs

    def slurm_output_directory(self):
        # the directory where submission meta data and logs should be stored
        return self.local_target(dir=True)

    def slurm_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        bootstrap_file = os.path.expandvars("$CF_BASE/columnflow/tasks/framework/remote_bootstrap.sh")
        if "CF_REMOTE_BOOTSTRAP_FILE" in os.environ:
            bootstrap_file = os.environ["CF_REMOTE_BOOTSTRAP_FILE"]

        # copy the file only once into the submission directory (sharing between jobs)
        # and allow rendering inside the job
        return law.JobInputFile(bootstrap_file, share=True, render_job=True)

    def slurm_job_config(self, config, job_num, branches):
        # add common config settings
        self.add_common_configs(
            config,
            {},
            law_config=False,
            voms=True,
            kerberos=True,
            wlcg=False,
        )

        # set job time
        if self.max_runtime is not None:
            job_time = law.util.human_duration(
                seconds=int(math.floor(self.max_runtime * 3600)) - 1,
                colon_format=True,
            )
            config.custom_content.append(("time", job_time))

        # set nodes
        config.custom_content.append(("nodes", 1))

        # custom, flavor dependent settings
        if self.slurm_flavor == "maxwell":
            # nothing yet
            pass

        # render variales
        config.render_variables["cf_bootstrap_name"] = "slurm"
        config.render_variables.setdefault("cf_pre_setup_command", "")
        config.render_variables.setdefault("cf_post_setup_command", "")

        # forward env variables
        for ev, rv in self.slurm_forward_env_variables.items():
            config.render_variables[rv] = os.environ[ev]

        return config

    def slurm_destination_info(self, info: dict[str, str]) -> dict[str, str]:
        info = super().slurm_destination_info(info)
        info = self.common_destination_info(info)
        return info


# prepare bases of the RemoteWorkflow container class
remote_workflow_bases = (HTCondorWorkflow, SlurmWorkflow)

if cf_flavor == "cms":
    from columnflow.tasks.cms.base import CrabWorkflow
    remote_workflow_bases += (CrabWorkflow,)


class RemoteWorkflow(*remote_workflow_bases):

    # upstream requirements
    reqs = Requirements(*(cls.reqs for cls in remote_workflow_bases))
