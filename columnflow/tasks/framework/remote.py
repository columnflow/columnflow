# coding: utf-8

"""
Base classes and tools for working with remote tasks and targets.
"""

import os
import re
import math

import luigi
import law

from columnflow.tasks.framework.base import AnalysisTask
from columnflow.util import real_path


class HTCondorWorkflow(law.htcondor.HTCondorWorkflow):

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
    htcondor_flavor = luigi.ChoiceParameter(
        default=os.getenv("CF_HTCONDOR_FLAVOR", "naf"),
        choices=("naf", "naf"),
        significant=False,
        description="the 'flavor' (i.e. configuration name) of the batch system; choices: "
        "naf,cern; default: '{}'".format(os.getenv("CF_HTCONDOR_FLAVOR", "naf")),
    )

    exclude_params_branch = {"max_runtime", "htcondor_cpus", "htcondor_flavor"}

    def htcondor_workflow_requires(self):
        reqs = law.htcondor.HTCondorWorkflow.htcondor_workflow_requires(self)

        # add bundles for repo, software and optionally cmssw sandboxes
        reqs["repo"] = BundleRepo.req(self, replicas=3)
        reqs["software"] = BundleSoftware.req(self, replicas=3)

        # get names of cmssw environments to bundle
        cmssw_sandboxes = None
        if getattr(self, "analysis_inst", None):
            cmssw_sandboxes = self.analysis_inst.get_aux("cmssw_sandboxes")
        if getattr(self, "config_inst", None):
            cmssw_sandboxes = self.config_inst.get_aux("cmssw_sandboxes", cmssw_sandboxes)
        if cmssw_sandboxes:
            reqs["cmssw"] = [
                BundleCMSSW.req(self, replicas=3, sandbox_file=f)
                for f in cmssw_sandboxes
            ]

        return reqs

    def htcondor_output_directory(self):
        # the directory where submission meta data and logs should be stored
        return self.local_target(dir=True)

    def htcondor_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        return os.path.expandvars("$CF_BASE/columnflow/tasks/framework/remote_bootstrap.sh")

    def htcondor_job_config(self, config, job_num, branches):
        # include the voms proxy
        proxy_file = law.wlcg.get_voms_proxy_file()
        if not law.wlcg.check_voms_proxy_validity(proxy_file=proxy_file):
            raise Exception("voms proxy not valid, submission aborted")
        config.input_files["proxy_file"] = proxy_file

        # include the wlcg specific tools script in the input sandbox
        config.input_files["wlcg_tools"] = law.util.law_src_path("contrib/wlcg/scripts/law_wlcg_tools.sh")

        # use cc7 at CERN (http://batchdocs.web.cern.ch/batchdocs/local/submit.html#os-choice)
        if self.htcondor_flavor == "cern":
            config.custom_content.append(("requirements", '(OpSysAndVer =?= "CentOS7")'))

        # some htcondor setups requires a "log" config, but we can safely set it to /dev/null
        # if you are interested in the logs of the batch system itself, set a meaningful value here
        config.custom_content.append(("log", "/dev/null"))

        # max runtime
        config.custom_content.append(("+MaxRuntime", int(math.floor(self.max_runtime * 3600)) - 1))

        # request cpus
        if self.htcondor_cpus > 0:
            config.custom_content.append(("RequestCpus", self.htcondor_cpus))

        # helper to return uris and a file pattern for replicated bundles
        reqs = self.htcondor_workflow_requires()
        def get_bundle_info(task):
            uris = task.output().dir.uri(cmd="filecopy", return_all=True)
            pattern = os.path.basename(task.get_file_pattern())
            return ",".join(uris), pattern

        # add software variables
        uris, pattern = get_bundle_info(reqs["software"])
        config.render_variables["cf_software_uris"] = uris
        config.render_variables["cf_software_pattern"] = pattern

        # add repo variables
        uris, pattern = get_bundle_info(reqs["repo"])
        config.render_variables["cf_repo_uris"] = uris
        config.render_variables["cf_repo_pattern"] = pattern

        # add cmssw sandbox variables
        config.render_variables["cf_cmssw_sandbox_uris"] = "()"
        config.render_variables["cf_cmssw_sandbox_patterns"] = "()"
        config.render_variables["cf_cmssw_sandbox_names"] = "()"
        if "cmssw" in reqs:
            info = [get_bundle_info(t) for t in reqs["cmssw"]]
            uris = [tpl[0] for tpl in info]
            patterns = [tpl[1] for tpl in info]
            names = [os.path.basename(t.sandbox_file)[:-3] for t in reqs["cmssw"]]
            config.render_variables["cf_cmssw_sandbox_uris"] = "({})".format(
                " ".join(map('"{}"'.format, uris)))
            config.render_variables["cf_cmssw_sandbox_patterns"] = "({})".format(
                " ".join(map('"{}"'.format, patterns)))
            config.render_variables["cf_cmssw_sandbox_names"] = "({})".format(
                " ".join(map('"{}"'.format, names)))

        config.render_variables["cf_bootstrap_name"] = "htcondor_standalone"
        config.render_variables["cf_htcondor_flavor"] = self.htcondor_flavor
        config.render_variables["cf_lcg_setup"] = os.environ["CF_LCG_SETUP"]
        config.render_variables["cf_base"] = os.environ["CF_BASE"]
        config.render_variables["cf_cern_user"] = os.environ["CF_CERN_USER"]
        config.render_variables["cf_store_name"] = os.environ["CF_STORE_NAME"]
        config.render_variables["cf_store_local"] = os.environ["CF_STORE_LOCAL"]
        config.render_variables["cf_local_scheduler"] = os.environ["CF_LOCAL_SCHEDULER"]
        config.render_variables["cf_store_local"] = os.environ["CF_STORE_LOCAL"]

        return config

    def htcondor_use_local_scheduler(self):
        # remote jobs should not communicate with ther central scheduler but with a local one
        return True


class BundleRepo(AnalysisTask, law.git.BundleGitRepository, law.tasks.TransferLocalFile):

    replicas = luigi.IntParameter(
        default=5,
        description="number of replicas to generate; default: 5",
    )
    version = None

    exclude_files = ["docs", "test", "data", ".law", ".setups", ".data"]

    task_namespace = None

    def get_repo_path(self):
        # required by BundleGitRepository
        return os.environ["CF_BASE"]

    def single_output(self):
        repo_base = os.path.basename(self.get_repo_path())
        return self.target(f"{repo_base}.{self.checksum}.tgz")

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else "*")

    def output(self):
        return law.tasks.TransferLocalFile.output(self)

    @law.decorator.log
    @law.decorator.safe_output
    def run(self):
        # create the bundle
        bundle = law.LocalFileTarget(is_tmp="tgz")
        self.bundle(bundle)

        # log the size
        self.publish_message("bundled repository archive, size is {:.2f} {}".format(
            *law.util.human_bytes(bundle.stat().st_size)))

        # transfer the bundle
        self.transfer(bundle)


class BundleSoftware(AnalysisTask, law.tasks.TransferLocalFile):

    replicas = luigi.IntParameter(
        default=5,
        description="number of replicas to generate; default: 5",
    )
    version = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._checksum = None

    @property
    def checksum(self):
        if not self._checksum:
            # get a list of all software flag files
            flag_files = []
            for venv_name in os.listdir(os.environ["CF_VENV_BASE"]):
                # skip all dev envs
                if venv_name.endswith("_dev"):
                    continue
                venv_flag = os.path.join(os.environ["CF_VENV_BASE"], venv_name, "cf_flag")
                flag_files.append(venv_flag)
            flag_files = sorted(set(map(os.path.realpath, flag_files)))

            # read content of all software flag files and create a hash
            contents = []
            for flag_file in flag_files:
                if os.path.isfile(flag_file):
                    with open(flag_file, "r") as f:
                        contents.append((flag_file, f.read().strip()))

            self._checksum = law.util.create_hash(contents)

        return self._checksum

    def single_output(self):
        return self.target(f"software.{self.checksum}.tgz")

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else "*")

    @law.decorator.log
    @law.decorator.safe_output
    def run(self):
        software_path = os.environ["CF_SOFTWARE"]

        # create the local bundle
        bundle = law.LocalFileTarget(software_path + ".tgz", is_tmp=True)

        def _filter(tarinfo):
            # skip hidden dev files
            if re.search(r"(\.pyc|\/\.git|\.tgz|__pycache__)$", tarinfo.name):
                return None
            # skip certain things manually
            if re.search(r"^\./venvs/cf_dev(|/.*)$", tarinfo.name):
                return None
            return tarinfo

        # create the archive with a custom filter
        bundle.dump(software_path, filter=_filter, formatter="tar")

        # log the size
        self.publish_message("bundled software archive, size is {:.2f} {}".format(
            *law.util.human_bytes(bundle.stat().st_size)))

        # transfer the bundle
        self.transfer(bundle)


class BundleCMSSW(AnalysisTask, law.cms.BundleCMSSW, law.tasks.TransferLocalFile):

    sandbox_file = luigi.Parameter(
        description="name of the sandbox file; when not absolute, the path is evaluated relative "
        "to $CF_BASE/sandboxes",
    )
    replicas = luigi.IntParameter(
        default=5,
        description="number of replicas to generate; default: 5",
    )
    version = None

    task_namespace = None
    exclude = "^src/tmp"

    def __init__(self, *args, **kwargs):
        # cached bash sandbox that wraps the cmssw environment
        self._cmssw_sandbox = None

        super().__init__(*args, **kwargs)

    @property
    def cmssw_sandbox(self):
        if self._cmssw_sandbox is None:
            env_file = real_path("$CF_BASE/sandboxes", self.sandbox_file)
            self._cmssw_sandbox = law.BashSandbox(env_file)

        return self._cmssw_sandbox

    def get_cmssw_path(self):
        return self.cmssw_sandbox.env["CMSSW_BASE"]

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else "*")

    def single_output(self):
        cmssw_path = os.path.basename(self.get_cmssw_path())
        return self.target(f"{cmssw_path}.{self.checksum}.tgz")

    def output(self):
        return law.tasks.TransferLocalFile.output(self)

    @law.decorator.log
    def run(self):
        # create the bundle
        bundle = law.LocalFileTarget(is_tmp="tgz")
        self.bundle(bundle)

        # log the size
        self.publish_message("bundled CMSSW archive, size is {:.2f} {}".format(
            *law.util.human_bytes(bundle.stat().st_size)))

        # transfer the bundle and mark the task as complete
        self.transfer(bundle)
