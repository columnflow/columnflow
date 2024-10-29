# __cf_analysis_name__ Analysis

# Quickstart

1. clone the analysis framework.
   > git clone --recursive ssh://git@gitlab.cern.ch:7999/ghentanalysis/columnflowanalysis/ttz.git

2. Setup the repository and install the environment.
   > cd ttz
   > source setup.sh

3. Create a GRID proxy if you intend to run tasks that need one
   > voms-proxy-init -rfc -valid 196:00

4. Checkout the 'Getting started' guide to run your first tasks.
https://github.com/GhentAnalysis/columnflow/blob/master/docs/user_guide/getting_started.md

# Datasets and processes

All processes and datasets are defined in the [cmsdb gitlab](https://gitlab.cern.ch/ghentanalysis/cmsdb/-/blob/master/cmsdb/).
Processes are added to the analysis in the file [config/processes.py](__cf_module_name__/config/processes.py). 
Data- and MC-sets are added to the analysis in the file [config/datasets.py](__cf_module_name__/config/datasets.py). 
Note the difference between a process and a dataset. A process can correspond to multiple datasets. The other way around is currently not possible.

The datasets (for ERA=a,b,c,d) are:
- data_mu_ERA corresponding to SingleMuon 
- data_mumu_ERA corresponding to DoubleMuon
- data_muoneg_ERA corresponding to MuonEG
- data_egamma_ERA corresponding to EGamma

The MC processes with corresponding datasets are
- ttbar with corresponding datasets tt_sl_powheg and tt_dl_powheg
- dy (Drell-Yan) with corresponding datasets dy_lept_m50_ht-RANGE_madgraph

The analysis can be run over only selected datasets using the --datasets argument. Groupings of datasets are defined in [config/datasets.py](__cf_module_name__/config/config___cf_short_name_lc__.py). A similar scheme exists for processes. 


# Object Definition

All objects collected in [selection/objects.py:object_selection](__cf_module_name__/selection/objects.py#L177).

## Muons

Defined in [selection/objects.py:muon_object](__cf_module_name__/selection/objects.py#L36).

- $|\eta| < 2.4$ 
- $p_T > 10$
- $\texttt{miniPFRelIso all} < 0.4$
- $\texttt{sip3d} < 8$
- $d_{xy} < 0.05$ 
- $d_z < 0.1$

Defined additionally Tight Muons::
- $\texttt{tightId}$

## Electrons

Defined in [selection/objects.py:electron_object](__cf_module_name__/selection/objects.py#L83).

- $|\eta| < 2.5$ 
- $p_T > 15$
- $\texttt{miniPFRelIso all} < 0.4$
- $\texttt{sip3d} < 8$
- $d_{xy} < 0.05$ 
- $d_z < 0.1$
- at most one lost hit 
- is a PF candidate
- with conversion veto applied 
- $\texttt{tightCharge} > 1$
- without a muon closeby ($\\Delta R < 0.05$)

## Jets

Defined in [selection/objects.py:jet_object](__cf_module_name__/selection/objects.py#L132).

- ak4 Jets (standard Jet collection in NanoAOD)
- $|\eta| < 2.5$ 
- $p_T > 30$
- $\texttt{jetId} \\ge 2$
- not containing a muon or lepton ($\\Delta R < 0.4$)


# Calibration

Currently only the JEC and JER corrections are implemented. Two procedures are defined:
- Full JEC uncertainies, no JER: [calibration/default.py:default](__cf_module_name__/calibration/default.py#L21).
- Only nominal JEC, but also JER: [calibration/default.py:skip_jecunc](__cf_module_name__/calibration/skip_jecunc.py#L50).

The applied procedure can be specified at 
[config/config___cf_short_name_lc__.py:cfg.x.default_calibrator](__cf_module_name__/config/config___cf_short_name_lc__.py#L339).


# Event selection

The aim is to select $t\overline{t}$ events. 
Full default selection flow collected in [selection/default.py:default](__cf_module_name__/selection/default.py#L213).
Different selections can be defined by writing a similar function, and changing the configuration at [config/config___cf_short_name_lc__.py:cfg.x.default_selector](__cf_module_name__/config/config___cf_short_name_lc__.py#L340).


- triggers applied in [selection/trigger.py:default](__cf_module_name__/selection/trigger.py#L57)
  - listed in [selection/trigger.py:add_triggers](__cf_module_name__/selection/trigger.py#L11)
- lepton selection applied in [selection/default.py:lepton_selection](__cf_module_name__/selection/default.py#L81).
    - remove Z resonance (same flavour, opposite sign, $|m_{\ell\ell} - 91| < 15$)
    - leading lepton $p_T > 30$
    - subleading lepton $p_T > 20$
    - all leptons in the event should be tight
- jet selection applied in  [selection/default.py:jet_selection](__cf_module_name__/selection/default.py#L136).
  - one b-tagged jet

Note that selections are calculated as masks but not yet applied. 

# Categories / channels

Four channels are defined in the configuration file, described in [config/categories.py](config/categories.py) and implemented in [categorization/example.py](__cf_module_name__/categorization/example.py).

- $ee$ [selection/categories.py:catid_selection_2e](__cf_module_name__/selection/categories.py#L24)
- $e\mu$ [selection/categories.py:catid_selection_1e1mu](__cf_module_name__/selection/categories.py#L33)
- $\mu\mu$ [selection/categories.py:catid_selection_2mu](__cf_module_name__/selection/categories.py#L42)
- inclusive [selection/categories.py:catid_selection_incl](__cf_module_name__/selection/categories.py#L14)


# Resources

- [columnflow](https://github.com/uhh-cms/columnflow)
- [law](https://github.com/riga/law)
- [order](https://github.com/riga/order)
- [luigi](https://github.com/spotify/luigi)

