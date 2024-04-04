# coding: utf-8

"""
Custom jet energy calibration methods that disable data uncertainties (for searches).
"""

from columnflow.calibration.cms.jets import jec


# custom jec calibrator that only runs nominal correction
jec_nominal = jec.derive("jec_nominal", cls_dict={"uncertainty_sources": []})