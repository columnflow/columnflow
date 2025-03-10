# coding: utf-8

"""
CMS-specific matching producers
"""

from __future__ import annotations

from columnflow.production.matching import delta_r_matcher


#
# derived matching producer classes for known use cases in CMS
#

# AK4 jet-to-gen-jet matcher
jet_gen_jet_matcher = delta_r_matcher.derive("jet_gen_jet_matcher", cls_dict=dict(
    src_name="Jet",
    dst_name="GenJet",
    output_idx_column="genJetIdx",
    max_dr=0.2,
    mc_only=True,
))

# AK8 jet-to-gen-jet matcher
fat_jet_gen_fat_jet_matcher = delta_r_matcher.derive("fat_jet_gen_fat_jet_matcher", cls_dict=dict(
    src_name="FatJet",
    dst_name="GenAK8Jet",
    output_idx_column="genAK8JetIdx",
    max_dr=0.4,
    mc_only=True,
))

# AK8 subjet-to-gen-subjet matcher
subjet_gen_subjet_matcher = delta_r_matcher.derive("subjet_gen_subjet_matcher", cls_dict=dict(
    src_name="SubJet",
    dst_name="SubGenJetAK8",
    output_idx_column="subGenJetAK8Idx",
    max_dr=0.2,
    mc_only=True,
))
