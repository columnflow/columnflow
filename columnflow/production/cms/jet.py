# coding: utf-8

"""
Jet-related producers.
"""

from __future__ import annotations

from dataclasses import dataclass

import law

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, load_correction_set
from columnflow.columnar_util import set_ak_column, layout_ak_array, flat_np_view

np = maybe_import("numpy")
ak = maybe_import("awkward")


@dataclass
class JetIdConfig:
    """
    Container object to describe a CMS jet id configuration, consisting of names of correction sets mapped to bit
    positions, similar to how the ``jetId`` column is defined in nanoAOD. Example:

    .. code-block:: python

        # configurtion for AK4 puppi jets
        # second bit for "tight" id, third bit for "tight + lepton veto" id
        JetIdConfig(corrections={
            "AK4PUPPI_Tight": 2,
            "AK4PUPPI_TightLeptonVeto": 3,
        })
    """

    corrections: dict[str, int]

    def __post_init__(self) -> None:
        # for each correction, check if the bit is set and fits into a uint8
        for cor_name, bit in self.corrections.items():
            if not (1 <= bit <= 8):
                raise ValueError(f"jet id bit must be between 1 and 8, got {bit} for {cor_name}")


@producer(
    # names of used and produced columns are added dynamically in init depending on jet_name
    # only run on mc
    mc_only=True,
    # name of the jet collection
    jet_name="Jet",
    # function to determine the correction file
    get_jet_id_file=(lambda self, external_files: external_files.jet_id),
    # function to determine the jet id config
    get_jet_id_config=(lambda self: self.config_inst.x.jet_id),
)
def jet_id(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Recomputes the jet id flag. Requires an external file in the config under ``jet_id``. Example:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "jet_id": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-120c4271/POG/JME/2022_Summer22/jetid.json.gz",  # noqa
        })

    *get_jet_id_file* can be adapted in a subclass in case it is stored differently in the external files.

    The pairs of correction set names and jet id bits (!) should be configured using the :py:class:`JetIdConfig` as an
    auxiliary entry in the config:

    .. code-block:: python

        from columnflow.production.cms.jet import JetIdConfig
        cfg.x.jet_id = JetIdConfig(
            corrections={
                "AK4PUPPI_Tight": 2,
                "AK4PUPPI_TightLeptonVeto": 3,
            },
        )

    *get_jet_id_config* can be adapted in a subclass in case it is stored differently in the config.

    Resources:

        - https://twiki.cern.ch/twiki/bin/view/CMS/JetID13p6TeV?rev=22#nanoAOD_Flags
        - https://cms-talk.web.cern.ch/t/bug-in-the-jetid-flag-in-nanov12-and-nanov13/108135
        - https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration/-/blob/120c4271917f30d67fb64c789eb91f7b52be4845/examples/jetidExample.py
    """ # noqa
    # compute flat inputs
    variable_map = {
        col: flat_np_view(events[self.jet_name][col], axis=1)
        for col in self.jet_columns
    }
    # sum of multiplicies might be required
    if "multiplicity" not in variable_map and "chMultiplicity" in variable_map and "neMultiplicity" in variable_map:
        variable_map["multiplicity"] = variable_map["chMultiplicity"] + variable_map["neMultiplicity"]

    # identify jets for which the evaluation can succeed
    valid_mask = self.get_valid_mask(variable_map)
    variable_map = {col: value[valid_mask] for col, value in variable_map.items()}

    # prepare the flat jet id array into which evaluated values will be inserted
    jet_id_flat = np.zeros(len(valid_mask), dtype=np.uint8)

    # iterate over all correctors
    for cor_name, pass_bit in self.cfg.corrections.items():
        inputs = [variable_map[inp.name] for inp in self.jet_id_correctors[cor_name].inputs]
        id_flag = self.jet_id_correctors[cor_name].evaluate(*inputs).astype(np.uint8)
        # the flag is either 0 or 1, so shift the bit to the correct position
        jet_id_flat[valid_mask] |= id_flag << (pass_bit - 1)

    # apply correct layout
    jet_id = layout_ak_array(jet_id_flat, events[self.jet_name].eta)

    # store them
    events = set_ak_column(events, f"{self.jet_name}.jetId", jet_id, value_type=np.uint8)

    return events


@jet_id.init
def jet_id_init(self: Producer, **kwargs) -> None:
    """
    Dynamically add the names of the used and produced columns depending on the jet name.
    """
    self.jet_columns = ["eta", "chHEF", "neHEF", "chEmEF", "neEmEF", "muEF", "chMultiplicity", "neMultiplicity"]
    self.uses.update(f"{self.jet_name}.{col}" for col in self.jet_columns)
    self.produces.add(f"{self.jet_name}.jetId")


@jet_id.requires
def jet_id_requires(self: Producer, task: law.Task, reqs: dict, **kwargs) -> None:
    """
    Adds the requirements needed the underlying task to recompute the jet id into *reqs*.
    """
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(task)


@jet_id.setup
def jet_id_setup(
    self: Producer,
    task: law.Task,
    reqs: dict,
    inputs: dict,
    reader_targets: law.util.InsertableDict,
    **kwargs,
) -> None:
    """
    Sets up the correction sets needed for the jet id using the external files.
    """
    bundle = reqs["external_files"]

    # get the jet id config
    self.cfg: JetIdConfig = self.get_jet_id_config()

    # create the correctors
    correction_set = load_correction_set(self.get_jet_id_file(bundle.files))
    self.jet_id_correctors = {cor_name: correction_set[cor_name] for cor_name in self.cfg.corrections}

    # store a lambda to identify good jets (a value of zero will be stored for others)
    self.get_valid_mask = lambda variable_map: variable_map["chMultiplicity"] >= 0


# derive with defaults for fatjets
fatjet_id = jet_id.derive("fatjet_id", cls_dict={
    "jet_name": "FatJet",
    "get_jet_id_config": (lambda self: self.config_inst.x.fatjet_id),
})


@producer(
    # uses, produces are specified in the init function
    jet_name="FatJet",
    subjet_name="SubJet",
    output_column="msoftdrop",
)
def msoftdrop(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Recalculates the softdrop mass for a given jet collection by computing the four-vector
    sum of the corresponding subjets.

    The *jet_name* parameter indicates the jet collection for which to recompute the softdrop mass.
    and the *subjet_name* parameter indicates the name of the corresponding subjet collection.
    The fields ``<jet_name>.subJetIdx1`` and ``<jet_name>.subJetIdx2`` must exist in the input array
    and yield the indices of the subjets matched to the main jets.
    """
    # retrieve input collections
    jet = events[self.jet_name]
    subjet = events[self.subjet_name]

    valid_subjets = []
    for subjet_idx_column in self.subjet_idx_columns:
        # mask negative subjet indices (= no subjet)
        subjet_idx = jet[subjet_idx_column]
        valid_subjet_idxs = ak.mask(subjet_idx, subjet_idx >= 0)

        # pad list of subjets to prevent index error on lookup
        padded_subjet = ak.pad_none(subjet, ak.max(valid_subjet_idxs) + 1)

        # retrieve subjets for each jet
        valid_subjet = padded_subjet[valid_subjet_idxs]
        valid_subjets.append(ak.singletons(valid_subjet, axis=-1))

    # merge lists for all sibjet index columns
    valid_subjets = ak.concatenate(valid_subjets, axis=-1)

    # attach coffea behavior so we can do LV arithmetic
    valid_subjets = ak.with_name(
        valid_subjets,
        "PtEtaPhiMLorentzVector",
        behavior=self.nano_behavior,
    )

    # recompute softdrop mass from LV sum
    valid_subjets_sum = valid_subjets.sum(axis=-1)
    msoftdrop = valid_subjets_sum.mass

    # set softdrop mass to -1 for jets that do not have
    # exactly 2 valid subjets
    n_valid_subjets = ak.num(valid_subjets, axis=-1)
    msoftdrop_masked = ak.where(
        n_valid_subjets == 2,
        msoftdrop,
        -1,
    )

    # store the softdrop mass in the specified output column
    events = set_ak_column(events, f"{self.jet_name}.{self.output_column}", msoftdrop_masked, value_type=np.float32)

    # return the event array
    return events


@msoftdrop.init
def msoftdrop_init(self: Producer, **kwargs) -> None:
    """
    Dynamically add `uses` and `produces`.
    """
    # input columns
    self.uses |= {
        f"{collection}.{var}"
        for collection in (self.jet_name, self.subjet_name)
        for var in ("pt", "eta", "phi", "mass")
    }

    self.subjet_idx_columns = ["subJetIdx1", "subJetIdx2"]
    self.uses |= {
        f"{self.jet_name}.{subjet_idx_column}"
        for subjet_idx_column in self.subjet_idx_columns
    }

    # outputs
    self.produces = {f"{self.jet_name}.{self.output_column}"}


@msoftdrop.setup
def msoftdrop_setup(self: Producer, task: law.Task, reqs: dict, **kwargs) -> None:
    import coffea

    self.nano_behavior = coffea.nanoevents.NanoAODSchema.behavior()
