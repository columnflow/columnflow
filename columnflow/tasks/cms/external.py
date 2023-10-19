# coding: utf-8

"""
CMS related tasks dealing with external data.
"""

from __future__ import annotations

import luigi
import law

from columnflow.types import Sequence
from columnflow.tasks.framework.base import Requirements, AnalysisTask, ConfigTask, wrapper_factory
from columnflow.tasks.external import BundleExternalFiles
from columnflow.util import safe_div


logger = law.logger.get_logger(__name__)


class CreatePileupWeights(ConfigTask):

    sandbox = "bash::$CF_BASE/sandboxes/cmssw_default.sh"

    data_mode = luigi.ChoiceParameter(
        default="hist",
        choices=["hist", "pileupcalc"],
        description="the mode to obtain the data pu profile; choices: 'hist', 'pileupcalc'; "
        "default: 'hist'",
    )
    version = None

    # upstream requirements
    reqs = Requirements(
        BundleExternalFiles=BundleExternalFiles,
    )

    def requires(self):
        return self.reqs.BundleExternalFiles.req(self)

    def output(self):
        return self.target(f"weights_from_{self.data_mode}.json")

    def sandbox_stagein(self):
        # stagein all inputs
        return True

    def sandbox_stageout(self):
        # stageout all outputs
        return True

    @law.decorator.log
    @law.decorator.safe_output
    def run(self):
        # prepare the external files and the output weights
        externals = self.requires()
        weights = {}

        # since this tasks uses stage-in into and stage-out from the sandbox,
        # prepare external files with the staged-in inputs
        externals.get_files(self.input())

        # read the mc profile
        mc_profile = self.read_mc_profile_from_cfg(externals.files.pu.mc_profile)

        # loop over nominal and minbias_xs shifts
        for shift in ["nominal", "minbias_xs_up", "minbias_xs_down"]:
            # read or create the data profile
            if self.data_mode == "hist":
                pu_hist_target = externals.files.pu.data_profile[shift]
                data_profile = self.read_data_profile_from_hist(pu_hist_target)
            else:  # "pileupcalc"
                pu_file_target = externals.files.pu.json
                mb_xs = self.config_inst.x.minbias_xs.get(shift)
                data_profile = self.read_data_profile_from_pileupcalc(pu_file_target, mb_xs)

            # build the ratios and save them
            if len(mc_profile) != len(data_profile):
                raise Exception(
                    f"the number of bins in the MC profile ({len(mc_profile)}) does not match that "
                    f"of the data profile for the {shift} shift ({len(data_profile)})",
                )

            # store them
            weights[shift] = [safe_div(d, m) for m, d in zip(mc_profile, data_profile)]
            self.publish_message(f"computed pu weights for shift {shift}")

        # save it
        self.output().dump(weights, formatter="json")

    @classmethod
    def read_mc_profile_from_cfg(
        cls,
        pu_config_target: law.FileSystemTarget,
    ) -> list[float]:
        """
        Takes a mc pileup configuration file stored in *pu_config_target*, parses its content and
        returns the pu profile as a list of float probabilities.
        """
        probs = []

        # read non-empty lines
        lines = map(lambda s: s.strip(), pu_config_target.load(formatter="text").split("\n"))

        # loop through them and extract probabilities
        found_prob_line = False
        for line in lines:
            # look for the start of the pu profile
            if not found_prob_line:
                if not line.startswith("mix.input.nbPileupEvents.probValue"):
                    continue
                found_prob_line = True
                line = line.split("(", 1)[-1].strip()
            # skip empty lines
            if not line:
                continue
            # extract numbers
            probs.extend(map(float, filter(bool, line.split(")", 1)[0].split(","))))
            # look for the end of the pu profile
            if ")" in line:
                break

        return cls.normalize_values(probs)

    @classmethod
    def read_data_profile_from_hist(
        cls,
        pu_hist_target: law.FileSystemTarget,
    ) -> list[float]:
        """
        Takes the pileup profile in data preproducd by the lumi pog and stored in *pu_hist_target*,
        builds the ratio to mc and returns the weights in a list.
        """
        hist_file = pu_hist_target.load(formatter="uproot")
        probs = hist_file["pileup"].values().tolist()
        return cls.normalize_values(probs)

    @classmethod
    def read_data_profile_from_pileupcalc(
        cls,
        pu_file_target: law.FileSystemTarget,
        minbias_xs: float,
    ) -> list[float]:
        """
        Takes the pileup profile in data read stored in *pu_file_target*, which should have been
        produced when processing data, and a *minbias_mexs* value in mb (milli), builds the ratio to
        mc and returns the weights in a list for 99 bins (recommended number).
        """
        raise NotImplementedError

    @classmethod
    def normalize_values(cls, values: Sequence[float]) -> list[float]:
        _sum = sum(values, 0.0)
        return [value / _sum for value in values]


CreatePileupWeightsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=CreatePileupWeights,
    enable=["configs", "skip_configs"],
    attributes={"version": None},
)
