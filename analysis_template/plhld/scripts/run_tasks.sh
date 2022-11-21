## just a small bash script for documentation 
## and to quickly run some default tasks

### Input parameter settings
: "
Many of these parameters have defaults (default_parameter) set in the config file,
which is used when no parameter is given on the command line. (e.g. default_selector='default')

Also, for CSV input parameters, most of the times you can use a 'parameter_group' instead,
which has to be defined in the config file. For example:
config.set_aux(processes_groups={
    'default': ['ggHH_kl_1_kt_1_sl_hbbhww', 'dy_lep', 'w_lnu', 'st', 'tt'], ...
})


There are also some parameters which are nice to check task dependencies and the status of tasks
(in general, no tasks are run when these commands are used)
--remove-output N,a      removes all outputs of the task and the task requirements up to depth N
--remove-output 0,a,y    removes output and re-runs task (nice for quickly changing plot styles)
--fetch-output 0,a       copies all task outputs into a local directory
--print-output N         prints output filenames of the task and requirements up to depth N
--print-deps N           prints name and input parameters of the task and requirements up to depth N
--print-status           checks if outputs of the task and requirements already exist up to depth N
--help                   if you need some information to some parameters, use this

Relevant parameters for job submission
--pilot                  disable requirements of the workflow (e.g. Reduce Events job also runs SelectEvents etc.)
--workers                no. of parallel workers (default: 1); when a task requires multiple workflows, we need 
                         multiple workers if we want the workflows to run in parallel
--workflow str           e.g. htcondor, local (check name)
--retries N              no. of automatic resubmissions (default: 5)
--tasks-per-job N        no. of tasks per job (default: 1)
--parallel-jobs N        max no. of parallel jobs running (default: inf)
--walltime N             max. wall time in hours (default: inf)
--max-runtime N          max. runtime in hours (default: 2)
--cancel-jobs True       cancel all submitted jobs (no resubmission)
--cleanup-jobs True      cleanup all submitted jobs (no resubmission)
"

## General (version must always be set, other parameters are optional)
version="v1"
workers=4
workflow=""
redo="--remove-output 0,a,y"

## Event processing
: "
These parameters should be fixed in general, so it should be fine to just use the defaults (skip on command line)
The input value of these parameters define which function is used in the calibration/selection/production etc.
If you write a new file containing such a function, you need to include it in the 'law.cfg'
"
calibrators="skip_jecunc"
selector="default"
producer="features"
ml_model="simple"
inference_model="default"

## config, dataset, processes, shifts
config="run2_2017"
dataset="ggHH_kl_1_kt_1_sl_hbbhww_powheg"
processes="working"
shift="nominal"
shift_sources="jer"

## Histogramming, Plotting
variables="m_bb,deltaR_bb"
cutflow_vars="cf_n_jet"
selector_steps="Lepton,Jet,Bjet,Trigger"
categories="incl"

## Plot styling
# Note: process_settings and plot_suffix are only in feature/PlotBase CF branch ATM
plot_suffix=""
#process_settings="ggHH_kl_1_kt_1_sl_hbbhww,scale=10000,unstack,label=HH:dy_lep,color=#377eb8"
process_settings="ggHH_kl_1_kt_1_sl_hbbhww,unstack:dy_lep,color=#377eb8"
yscale="linear"
skip_cms="False"
skip_ratio="False"
shape_norm="False"
# rebin=1  # TODO include in CF

## Select and reduce events (already included in the workflow, but included as an example)
## (branch 0 means that only the 1st file of the NanoAOD is processed)
: "
law run cf.SelectEvents --version $version \
    --calibrators $calibrators --selector $selector \
    --dataset tt_sl_powheg \
    --branch 0

law run cf.ReduceEvents --version $version \
    --calibrators $calibrators --selector $selector \
    --dataset tt_sl_powheg \
    --branch 0
" 

## When the calibration and selection is fixed, we should run ReduceEvents once
## for all relevant datasets, shifts and configs
## 2017: data,tt,st,w_lnu,dy_lep and hh datasets add up to ~2150 files (1 job per file and systematic)
relevant_datasets="*"
relevant_shifts="nominal"
relevant_configs="run2_2017"
: "
law run cf.ReduceEventsWrapper --version $version --workers $workers \
    --cf.ReduceEvents-calibrators $calibrators --cf.ReduceEvents-selector $selector \
    --configs $relevant_configs \
    --shifts $relevant_shifts \
    --datasets $relevant_datasets \
    --cf.ReduceEvents-workflow htcondor --cf.ReduceEvents-pilot True \
    --cf.ReduceEvents-parallel-jobs 4000 --cf.ReduceEvents-tasks-per-job 1
"

## create a simple cutflow plot

law run cf.PlotCutflow --version $version \
    --workers $workers \
    --calibrators $calibrators --selector $selector \
    --shift $shift \
    --processes $processes \
    --categories $categories \
    --selector-steps $selector_steps \
    --process-settings $process_settings \
    --yscale linear \
    --shape-norm True \
    --skip-cms $skip_cms


## create cutflow variables plots
# 'per-plot steps' creates 1 plot per process (all steps)
# 'per-plot processes' creates 1 plot per selector step (all processes)

law run cf.PlotCutflowVariables --version $version --per-plot steps \
    --workers $workers \
    --calibrators $calibrators --selector $selector \
    --shift $shift \
    --variables $cutflow_vars \
    --processes $processes \
    --categories $categories \
    --selector-steps $selector_steps \
    --process-settings $process_settings \
    --yscale linear \
    --skip-ratio True \
    --shape-norm False \
    --skip-cms $skip_cms

law run cf.PlotCutflowVariables --version $version --per-plot processes \
    --workers $workers \
    --calibrators $calibrators --selector $selector \
    --shift $shift \
    --variables $cutflow_vars \
    --processes $processes \
    --categories $categories \
    --selector-steps $selector_steps \
    --process-settings $process_settings \
    --yscale linear \
    --skip-ratio False \
    --shape-norm False \
    --skip-cms $skip_cms

## create plots after the full workflow (with full eventweights applied)

law run cf.PlotVariables --version $version \
    --workers $workers \
    --calibrators $calibrators  --selector $selector --producer $producer \
    --shift $shift \
    --variables $variables \
    --processes $processes \
    --categories $categories \
    --process-settings $process_settings \
    --yscale linear \
    --skip-ratio False \
    --shape-norm True \
    --skip-cms $skip_cms


# create plots for nominal + up/down variations

law run cf.PlotShiftedVariables --version $version \
    --workers $workers \
    --calibrators $calibrators --selector $selector --producer $producer \
    --shift-sources $shift_sources \
    --variables $variables \
    --processes $processes \
    --categories $categories \
    --process-settings $process_settings \
    --yscale linear \
    --skip-ratio False \
    --shape-norm False \
    --skip-cms $skip_cms

# create datacards

law run cf.CreateDatacards --version $version \
    --workers $workers \
    --inference-model $inference_model \
    --calibrators $calibrators --selector $selector --producers $producer
