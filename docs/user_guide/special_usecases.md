# Special Usecases

In this section, additional usecases will presented.

Note: categorization after ML tasks, ...

TODO

## Mechanism to hook into histograms (e.g. for ABCD methods)

This section describes a mechanism to hook into the histogram creation process.
This is useful for data-driven background estimations, e.g. based on ABCD methods.

### Hist hooks

A hist hook can be defined in a aux entry `hist_hooks` of the config.

```python
def example_hook(
    task: AnalysisTask,
    hists: dict[od.Process, hist.Hist],
) -> dict[od.Process, hist.Hist]:
    # add new objects to hists and return it
    return hists


cfg.x.hist_hooks = {
    "example": example_hook,
}
```

By appending the optional parameter ```--hist-hook NAME``` to the task call in the CL, the hook can be triggered right before the plotting function is invoked and also right before certain axis are projected or selected.

```bash
law run cf.PlotVariables1D --version v1 --processes tt,st --variables jet1_pt --hist-hook example
```

This is supported by all non-cutflow plot tasks.
In the example of the `cf.PlotVariables1D`, the histograms being passed to the hook still contain the `category` and, optionally, the `shift` axes.
This way, hist hooks can easily be used to inject new histograms from data-driven background estimations based on existing histograms.

:::{dropdown} Example: QCD estimation

A trivial example in which a process "QCD" should be estimated as half of the "tt" contribution would look like:

```python
def example_hook(
    task: AnalysisTask,
    hists: dict[od.Process, hist.Hist],
) -> dict[od.Process, hist.Hist]:
    # create a new "QCD" process, if not already done in the config itself
    import order as od
    qcd = od.Process("qcd", id="+", label="QCD", color1=(244, 93, 66))

    # create the qcd shape
    tt_hist = hists[self.config_inst.processes.n.tt]
    hists[qcd] = tt_hist * 0.5

    return hists
```

:::

## Setting default Calibrator, Selector, Producer, Ml-Model (CSPM) via function

It is useful sometimes to dynamically choose which CSPM to be triggered in the workflow based on the given task parameters or model parameters.
In columnflow it is possible to link the default CSPM to a function, which return value is set as default during the runtime of the task.
The function takes all task parameters as inputs and can be structured as needed

With this functionality, you can also set multiple CSPs as your defaults.
For tasks that can only use one CSP, the first one is used.

The function's return value can be None (no default is set), a string or a list of strings, where each string is the name of a valid CSPM.
A simple function can look like

```python
def default_task(
    container: law.task.base.Register,
    config: order.config.Config,
    task_parameters: collections.OrderedDict
) -> None | str | List[str]:
    """ Function that chooses the default CSPM based on the task parameters """
    default_task = None
    # Do something
    return default_task
```

:::{dropdown} Example 1: choosing the ML-Model based on some default that is set in the inference model

```python
from columnflow.inference import InferenceModel

def default_ml_model(container, **task_params):
    """ Function that chooses the default_ml_model based on the inference_model if given """
    default_ml_model = None

    # check if task is using an inference model
    if "inference_model" in task_params.keys():
        inference_model = task_params.get("inference_model", None)

        # if inference model is not set, assume it's the container default
        if inference_model in (None, "NO_STR"):
            inference_model = container.x.default_inference_model

        # get the default_ml_model from the inference_model_inst
        inference_model_inst = InferenceModel._subclasses[inference_model]
        default_ml_model = getattr(inference_model_inst, "ml_model_name", default_ml_model)

        return default_ml_model

    return default_ml_model

config_inst.x.default_ml_model = default_ml_model
```

:::

:::{dropdown} Example 2: extend your default producers based on which ML Model is used

```python
def default_producers(container, **task_params):
    """ Default producers chosen based on the Inference model and the ML Model """

    # per default, use the ml_inputs and event_weights
    default_producers = ["ml_inputs", "event_weights"]

    # check if a ml_model has been set
    ml_model = task_params.get("mlmodel", None) or task_params.get("mlmodels", None)

    # only consider 1 ml_model
    if isinstance(ml_model, (list, tuple)):
        ml_model = ml_model[0]

    # try and get the default ml model if not set
    if ml_model in (None, "NO_STR"):
        ml_model = default_ml_model(container, **task_params)

    # if a ML model is set, add some producer that depends on the ML Model (e.g. for categorising based on ML outputs)
    if ml_model not in (None, "NO_STR"):
        default_producers.insert(0, f"ml_{ml_model}")

    return default_producers

config_inst.x.default_producer = default_producers
```

:::
