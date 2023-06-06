# coding: utf-8

"""
Base classes and tools for working with remote tasks and targets.
"""

import os
import re
import math

import luigi
import law

from columnflow.tasks.framework.base import Requirements, AnalysisTask
from columnflow.util import real_path


class BundleRepo(AnalysisTask, law.git.BundleGitRepository, law.tasks.TransferLocalFile):

    replicas = luigi.IntParameter(
        default=5,
        description="number of replicas to generate; default: 5",
    )
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


class BuildBashSandbox(AnalysisTask):

    sandbox_file = luigi.Parameter(
        description="the sandbox file to install",
    )
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
            path = os.path.expandvars(os.path.expanduser(params["sandbox_file"]))
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
    version = None

    # upstream requirements
    reqs = Requirements(
        BuildBashSandbox=BuildBashSandbox,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # get the name and install path of the sandbox
        from cf_sandbox_file_hash import create_sandbox_file_hash
        sandbox_file = os.path.expandvars(os.path.expanduser(self.sandbox_file))
        self.sandbox_file_hash = create_sandbox_file_hash(sandbox_file)
        self.venv_name = os.path.splitext(os.path.basename(sandbox_file))[0]
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


class BundleCMSSWSandbox(AnalysisTask, law.cms.BundleCMSSW, law.tasks.TransferLocalFile):

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

    # upstream requirements
    reqs = Requirements(
        BuildBashSandbox=BuildBashSandbox,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # get the name and install path of the sandbox
        from cf_sandbox_file_hash import create_sandbox_file_hash
        sandbox_file = os.path.expandvars(os.path.expanduser(self.sandbox_file))
        self.sandbox_file_hash = create_sandbox_file_hash(sandbox_file)
        self.cmssw_env_name = os.path.splitext(os.path.basename(sandbox_file))[0]
        self.cmssw_env_name_hashed = f"{self.cmssw_env_name}_{self.sandbox_file_hash}"

    def requires(self):
        return self.reqs.BuildBashSandbox.req(self)

    def get_cmssw_path(self):
        # invoking .env will already trigger building the sandbox
        return self.requires().sandbox_inst.env["CMSSW_BASE"]

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


_default_htcondor_flavor = law.config.get_expanded("analysis", "htcondor_flavor", "cern")
_default_htcondor_share_software = law.config.get_expanded_boolean("analysis", "htcondor_share_software", False)


class HTCondorWorkflow(AnalysisTask, law.htcondor.HTCondorWorkflow):

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
        choices=("naf", "cern"),
        significant=False,
        description="the 'flavor' (i.e. configuration name) of the batch system; choices: "
        f"naf,cern; default: '{_default_htcondor_flavor}'",
    )
    htcondor_share_software = luigi.BoolParameter(
        default=_default_htcondor_share_software,
        significant=False,
        description="when True, do not bundle and download software plus sandboxes but instruct "
        "jobs to use the software in the current CF_SOFTWARE_BASE if accessible; default: "
        f"{_default_htcondor_share_software}",
    )

    exclude_params_branch = {
        "max_runtime", "htcondor_cpus", "htcondor_gpus", "htcondor_memory",
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
        "CF_VOMS": "cf_voms",
    }

    # upstream requirements
    reqs = Requirements(
        BundleRepo=BundleRepo,
        BundleSoftware=BundleSoftware,
        BuildBashSandbox=BuildBashSandbox,
        BundleBashSandbox=BundleBashSandbox,
        BundleCMSSWSandbox=BundleCMSSWSandbox,
    )

    def htcondor_workflow_requires(self):
        reqs = law.htcondor.HTCondorWorkflow.htcondor_workflow_requires(self)

        # add the repository bundle
        reqs["repo"] = self.reqs.BundleRepo.req(self)

        # main software stack
        if not self.htcondor_share_software:
            reqs["software"] = self.reqs.BundleSoftware.req(self)

        # get names of pure bash and cmssw sandboxes
        bash_sandboxes = None
        cmssw_sandboxes = None
        if getattr(self, "analysis_inst", None):
            bash_sandboxes = self.analysis_inst.x("bash_sandboxes", [])
            cmssw_sandboxes = self.analysis_inst.x("cmssw_sandboxes", [])
        if getattr(self, "config_inst", None):
            bash_sandboxes = self.config_inst.x("bash_sandboxes", bash_sandboxes)
            cmssw_sandboxes = self.config_inst.x("cmssw_sandboxes", cmssw_sandboxes)

        # bash-based sandboxes
        cls = self.reqs.BuildBashSandbox if self.htcondor_share_software else self.reqs.BundleBashSandbox
        reqs["bash_sandboxes"] = [
            cls.req(self, sandbox_file=sandbox_file)
            for sandbox_file in bash_sandboxes
        ]

        # optional cmssw sandboxes
        if cmssw_sandboxes:
            cls = self.reqs.BuildBashSandbox if self.htcondor_share_software else self.reqs.BundleCMSSWSandbox
            reqs["cmssw_sandboxes"] = [
                cls.req(self, sandbox_file=sandbox_file)
                for sandbox_file in cmssw_sandboxes
            ]

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
        # include the voms proxy if not skipped
        if not law.config.get_expanded_boolean("analysis", "skip_ensure_proxy", default=False):
            voms_proxy_file = law.wlcg.get_voms_proxy_file()
            if not law.wlcg.check_voms_proxy_validity(proxy_file=voms_proxy_file):
                raise Exception("voms proxy not valid, submission aborted")
            config.input_files["voms_proxy_file"] = law.JobInputFile(
                voms_proxy_file,
                share=True,
                render=False,
            )

        # include the wlcg specific tools script in the input sandbox
        config.input_files["wlcg_tools"] = law.JobInputFile(
            law.util.law_src_path("contrib/wlcg/scripts/law_wlcg_tools.sh"),
            share=True,
            render=False,
        )

        # some htcondor setups requires a "log" config, but we can safely set it to /dev/null
        # if you are interested in the logs of the batch system itself, set a meaningful value here
        config.custom_content.append(("log", "/dev/null"))

        # use cc7 at CERN (https://batchdocs.web.cern.ch/local/submit.html)
        if self.htcondor_flavor == "cern":
            config.custom_content.append(("requirements", "(OpSysAndVer =?= \"CentOS7\")"))

        # same on naf with case insensitive comparison (https://confluence.desy.de/display/IS/BIRD)
        if self.htcondor_flavor == "naf":
            config.custom_content.append(("requirements", "(OpSysAndVer == \"CentOS7\")"))

        # maximum runtime, compatible with multiple batch systems
        max_runtime = int(math.floor(self.max_runtime * 3600)) - 1
        config.custom_content.append(("+MaxRuntime", max_runtime))
        config.custom_content.append(("+RequestRuntime", max_runtime))

        # request cpus
        if self.htcondor_cpus > 0:
            config.custom_content.append(("RequestCpus", self.htcondor_cpus))

        # request gpus
        if self.htcondor_gpus > 0:
            # TODO: the exact setting might be flavor dependent in the future
            # e.g. https://confluence.desy.de/display/IS/GPU+on+NAF
            config.custom_content.append(("Request_GPUs", self.htcondor_gpus))

        # request memory
        if self.htcondor_memory > 0:
            config.custom_content.append(("Request_Memory", self.htcondor_memory))

        # helper to return uris and a file pattern for replicated bundles
        reqs = self.htcondor_workflow_requires()
        join_bash = lambda seq: " ".join(map('"{}"'.format, seq))
        def get_bundle_info(task):
            uris = task.output().dir.uri(base_name="filecopy", return_all=True)
            pattern = os.path.basename(task.get_file_pattern())
            return ",".join(uris), pattern

        # add repo variables
        uris, pattern = get_bundle_info(reqs["repo"])
        config.render_variables["cf_repo_uris"] = uris
        config.render_variables["cf_repo_pattern"] = pattern

        if self.htcondor_share_software:
            config.render_variables["cf_software_base"] = os.environ["CF_SOFTWARE_BASE"]
        else:
            # add software variables
            uris, pattern = get_bundle_info(reqs["software"])
            config.render_variables["cf_software_uris"] = uris
            config.render_variables["cf_software_pattern"] = pattern

            # add bash sandbox variables
            uris, patterns = law.util.unzip([get_bundle_info(t) for t in reqs["bash_sandboxes"]])
            names = [
                os.path.splitext(os.path.basename(t.sandbox_file))[0]
                for t in reqs["bash_sandboxes"]
            ]
            config.render_variables["cf_bash_sandbox_uris"] = join_bash(uris)
            config.render_variables["cf_bash_sandbox_patterns"] = join_bash(patterns)
            config.render_variables["cf_bash_sandbox_names"] = join_bash(names)

            # add cmssw sandbox variables
            config.render_variables["cf_cmssw_sandbox_uris"] = ""
            config.render_variables["cf_cmssw_sandbox_patterns"] = ""
            config.render_variables["cf_cmssw_sandbox_names"] = ""
            if "cmssw_sandboxes" in reqs:
                uris, patterns = law.util.unzip([get_bundle_info(t) for t in reqs["cmssw_sandboxes"]])
                names = [
                    os.path.splitext(os.path.basename(t.sandbox_file))[0]
                    for t in reqs["cmssw_sandboxes"]
                ]
                config.render_variables["cf_cmssw_sandbox_uris"] = join_bash(uris)
                config.render_variables["cf_cmssw_sandbox_patterns"] = join_bash(patterns)
                config.render_variables["cf_cmssw_sandbox_names"] = join_bash(names)

        # other render variables
        config.render_variables["cf_bootstrap_name"] = "htcondor_standalone"
        config.render_variables["cf_htcondor_flavor"] = self.htcondor_flavor
        config.render_variables.setdefault("cf_pre_setup_command", "")
        config.render_variables.setdefault("cf_post_setup_command", "")

        # forward env variables
        for ev, rv in self.htcondor_forward_env_variables.items():
            config.render_variables[rv] = os.environ[ev]

        return config

    def htcondor_use_local_scheduler(self):
        # remote jobs should not communicate with ther central scheduler but with a local one
        return True


_default_slurm_flavor = law.config.get_expanded("analysis", "slurm_flavor", "maxwell")
_default_slurm_partition = law.config.get_expanded("analysis", "slurm_partition", "cms-uhh")


class SlurmWorkflow(AnalysisTask, law.slurm.SlurmWorkflow):

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

        # get names of pure bash and cmssw sandboxes
        bash_sandboxes = None
        cmssw_sandboxes = None
        if getattr(self, "analysis_inst", None):
            bash_sandboxes = self.analysis_inst.x("bash_sandboxes", [])
            cmssw_sandboxes = self.analysis_inst.x("cmssw_sandboxes", [])
        if getattr(self, "config_inst", None):
            bash_sandboxes = self.config_inst.x("bash_sandboxes", bash_sandboxes)
            cmssw_sandboxes = self.config_inst.x("cmssw_sandboxes", cmssw_sandboxes)

        # bash-based sandboxes
        reqs["bash_sandboxes"] = [
            self.reqs.BuildBashSandbox.req(self, sandbox_file=sandbox_file)
            for sandbox_file in bash_sandboxes
        ]

        # optional cmssw sandboxes
        if cmssw_sandboxes:
            reqs["cmssw_sandboxes"] = [
                self.reqs.BuildBashSandbox.req(self, sandbox_file=sandbox_file)
                for sandbox_file in cmssw_sandboxes
            ]

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
        # include the voms proxy if not skipped
        if not law.config.get_expanded_boolean("analysis", "skip_ensure_proxy", default=False):
            voms_proxy_file = law.wlcg.get_voms_proxy_file()
            if os.path.exists(voms_proxy_file):
                config.input_files["voms_proxy_file"] = law.JobInputFile(
                    voms_proxy_file,
                    share=True,
                    render=False,
                )

        # include the kerberos ticket when existing
        if "KRB5CCNAME" in os.environ:
            kfile = os.environ["KRB5CCNAME"]
            kerberos_proxy_file = os.sep + kfile.split(os.sep, 1)[-1]
            if os.path.exists(kerberos_proxy_file):
                config.input_files["kerberos_proxy_file"] = law.JobInputFile(
                    kerberos_proxy_file,
                    share=True,
                    render=False,
                )

        # set job time and nodes
        job_time = law.util.human_duration(
            seconds=int(math.floor(self.max_runtime * 3600)) - 1,
            colon_format=True,
        )
        config.custom_content.append(("time", job_time))
        config.custom_content.append(("nodes", 1))

        # custom, flavor dependent settings
        if self.slurm_flavor == "maxwell":
            # extend kerberos privileges to afs on NAF
            if "kerberos_proxy_file" in config.input_files:
                config.render_variables["cf_pre_setup_command"] = "aklog"

        # render variales
        config.render_variables["cf_bootstrap_name"] = "slurm"
        config.render_variables.setdefault("cf_pre_setup_command", "")
        config.render_variables.setdefault("cf_post_setup_command", "")
        # custom tmp dir since slurm uses the job submission dir as the main job directory, and law
        # puts the tmp directory in this job directory which might become quite long; then,
        # python's default multiprocessing puts socket files into that tmp directory which comes
        # with the restriction of less then 80 characters that would be violated, and potentially
        # would also overwhelm the submission directory
        config.render_variables["law_job_tmp"] = "/tmp/law_$( basename \"$LAW_JOB_HOME\" )"

        # forward env variables
        for ev, rv in self.slurm_forward_env_variables.items():
            config.render_variables[rv] = os.environ[ev]

        return config


class RemoteWorkflow(HTCondorWorkflow, SlurmWorkflow):

    # upstream requirements
    reqs = Requirements(
        HTCondorWorkflow.reqs,
        SlurmWorkflow.reqs,
    )
