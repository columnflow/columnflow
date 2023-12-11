# Machine Learning

In this section, the users will learn how to implement machine learning in their analysis with columnflow.

# General procedure:
In columnflow, the utilization of machine learning models involves a two-step process:
- defining a class
- initialize an instance of that class with your trainings config

# Step 1: Defining your custom machine learning class:
To create a custom machine learning (ML) class in columnflow, it is imperative to inherit from the MLModel class.
This inheritance ensures the availability of functions to manage and access config and model instances, as well as the necessary producers.

The name of your custom ML class can be arbitrary, e.g:
```python
# define your model
class TestModel(MLModel):
    ...

```
In following we will go through several abstract functions that you must overwrite, in order to be able to use your custom ML class with columnflow.

# ABC functions

## sandbox:
In the `sandbox`, you specify which `sandbox` setup file should be sourced to configure the environments.
The return value of `sandbox` is the path to your shell file, e.g:
```{literalinclude} ./ml_code.py
:language: python
:start-at: def sandbox
:end-at: return "bash::$CF_BASE/sandboxes/venv_ml_tf.sh"
```
REMOVE
```python
def sandbox(self, task: law.Task) -> str:
        return "bash::$CF_BASE/sandboxes/venv_ml_tf.sh"
```
To keep things organized, store your `sandbox` setup and requirement file in `$CF_BASE/sandboxes/`.
It is also recommended to start the path with `bash::`, to indicate that you want to source the `sandbox` with `bash`.

The content of the `sandbox` setup file is a modification of the `cf.sh`.
So copy this file and modify the copy, by change `export CF_VENV_REQUIREMENTS` to point to your `requirement.txt`, e.g:
```bash
action() {
    local shell_is_zsh=$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # set variables and source the generic venv setup
    export CF_SANDBOX_FILE="${CF_SANDBOX_FILE:-${this_file}}"
    export CF_VENV_NAME="$( basename "${this_file%.sh}" )"

    # only thing you need to change
    export CF_VENV_REQUIREMENTS="${this_dir}/ml_tf_training.txt"

    source "${CF_BASE}/sandboxes/_setup_venv.sh" "$@"

    export TF_CPP_MIN_LOG_LEVEL="3"
}
action "$@"
```

The `requirement.txt` uses pip notation, e.g:
```bash
# version 1
awkward~=2.0
uproot~=5.0
git+https://github.com/riga/coffea.git@9ecdee5#egg=coffea
dask-awkward~=2023.2
tensorflow~=2.11
```
Columnflow manages sandboxes by using a version number at very first line, styled as:`# version ANY_FLOAT`.
This version defines a software packet, and it is good practice to change the version number, whenever environment is altered.

For more information, refer to the official [pip documentation](https://pip.pypa.io/en/stable/reference/requirements-file-format/).

One may ask, how to work with namespaces of certain modules, if it is not guarantee, that this module is available.
For this case `maybe_import` is there.
This utility function handles the import for you and makes the namespace available.
Remember to use this within a function if you use the module once, and at the beginning if you intend to use it by multiple functions.
The input of `maybe_import` is the name of the module, but as string.
For example this tutorial uses TensorFlow in multiple occurrences, and therefore `maybe_import` of this module is right at the start:
```python
tf = maybe_import("tensorflow")
```

## datasets:
In the `datasets` function, you specify which datasets are important for your machine learning model and which dataset instance should be extracted from your config.
To use this function your datasets needs to be added to your campaign, as defined by the `Order` Module.
An example can be found (here)[https://github.com/uhh-cms/cmsdb/blob/d83fb085d6e43fe9fc362df75bbc12816b68d413/cmsdb/campaigns/run2_2018_nano_uhh_v11/top.py].
It is recommended to return this as `set`, to prevent double counting.
```{literalinclude} ./ml_code.py
:language: python
:start-at: def datasets
:end-at: return set(dataset_inst)
```

REMOVE
```python
def datasets(self, config_inst: od.Config) -> set[od.Dataset]:
    # normally you would pass this to the model via config and loop through these names ...
    all_datasets_names = self.dataset_names

    dataset_inst = []
    for dataset_name in all_datasets:
        dataset_inst.append(config_inst.get_dataset(dataset_name))

    # ... but you can also add one dataset by using its name
    config_inst.get_dataset("tt_sl_powheg")

return set(dataset_inst)
```

## produces:
By default, intermediate columns are not saved within the columnflow framework but are filtered out afterwards.
If you want to prevent certain columns from being filtered out, you need to tell columnflow by writing them in the `produces`function.
This is always done by writing an iterable containing the name of the producer as string.
The names are of the form "**class_name_that_produces_the_column**.**name_of_the_column**".

In the following example, I want to preserve the number of muons and electrons, but we also want to preserve for example the output of the neural network.
To avoid confusion, we are not producing the columns in this function, we only tell columnflow to not throwing them away.
For more information about producers look into [TODO add PRODUCER EXAMPLE from NATHAN]()
```{literalinclude} ./ml_code.py
:language: python
:start-at: def produces
:end-at: return preserved_columns
```

REMOVE
```python
    def produces(self, config_inst: od.Config) -> set[Route | str]:
        preseve_columns = {
            f"{self.cls_name}.n_muon",
            f"{self.cls_name}.n_electron",
            f"{self.cls_name}.ml_prediction",
        }
        return
```

## output:
In the `output` function, you define your local target directory that your current model instance will have access to.

Since machine learning in columnflow uses k-fold by default, it is a good idea to have a separate directory for each fold, and this should be reflected in the `output` path.
It is of good practice to use `task.target` to be that your "machine-learning-task" related files are saved in the same directory.
In this example we want to save each fold separately e.g:
```{literalinclude} ./ml_code.py
:language: python
:start-at: def output
:end-at: return dst_path
```

REMOVE
```python
    def output(self, task: law.Task) -> Any:
        current_fold = task.fold
        # needs to be given config
        max_folds = self.folds
        # create dir at task.target if it does not exist
        dst_path = task.target(f"mlmodel_f{current_fold}of{max_folds}", dir=True)
        return dst_path
```

## open_model:
In the `open_model` function, you implement the loading of the trained model and if necessary its configuration, so it is ready to use for `evaluate`.
This does not define how the model is build for `train`.

The `target` parameter represents the local path to the models directory.
In the the following example a TensorFlow model saved with Keras API is loaded and returned, and no further configuration happens:
```{literalinclude} ./ml_code.py
:language: python
:start-at: def open_model
:end-at: return model
```

REMOVE
```python
    def open_model(self, target: law.LocalDirectoryTarget) -> tf.keras.models.Model:
        model = tf.keras.models.load_model(target.path)
        return model
```

## train:
In the `train` function, you implement the initialization of your models and the training loop.
By default train has access to the location of the models `inputs` and `outputs`.

In columnflow, k-fold is enabled by default.
The `Self` argument in `train` referes to the instance of the fold.
Using `Self`, you have also access to the entire `analysis_inst`ance the `config_inst`ance of the current fold, and to all the derived parameters of your model.

With this information, you can call and prepare the columns to be used by the model for training.
It is considered good practice to define the model building and column preprocessing in functions outside of `train`, keeping the definition of the trainings loop and the preparation of the model or the data apart.
An example about how to prepare columns is can be found [here](TODO Link)

E.g. for a very simple trainings loop using Keras fit function.
```{literalinclude} ./ml_code.py
:language: python
:start-at: def train
:end-at: return
```



```python
def train(
        self,
        task: law.Task,
        input: dict[str, list[law.FileSystemFileTarget]],
        output: law.FileSystemDirectoryTarget,
                ) -> None:
            # get access to tf module in sandbox
            tf = maybe_import("tensorflow")

            # prepare your trainings data
            train_x, train_y = prepare_events(task, input)

            # init a neural network instance
            model = prepare_ml_model()

            # make network ready for training
            model.compile(
                optimizer="Adam",
                loss="mse",
                )

            # train your model
            model.fit(
                train_x,
                train_y,
                )

            # dump the model at your output directory
            output.dump(
                model,
                formatter="tf_keras_model",
                )

            # now would be the best time to save metrics, plots and so on...
```

## evaluate:
Within `train` one defined the trainings process of the ML model, while in `evaluate` its evaluation is defined.
The corresponding `task` is `MLEvaluation` and depends on `MLTraining` and will therefore trigger a training if no training was performed before.

For each fold of the k-folds a neural network model is trained and can be accessed from `models`.
The actual loading, of the trained model stored in the list, is defined in `open_model` function.

The awkward array `events` is loaded in chunks and contains the merged events of all folds.
To filter out the test set, create a mask with `fold_indices`.

If you want to preserve columns and write them out into a parquet file, append the columns to `events` using `set_ak_column` and return `events`.
All columns not present in `produces` are then filtered out.

In the following example, the models prediction as well as the number of muons and electrons are saved, all the other columns in events are thrown away, since they are not present in `use`:


<!-- TODO Ask Matthis when this is set. Cant find it. -->
Don't confuse this behavior with the parameter `events_used_in_training`.
This flag determines if a certain `dataset` and shift combination can be used by the task.

```{literalinclude} ./ml_code.py
:language: python
:start-at: def evaluate
:end-at: return events
```


```python
    def evaluate(
        self,
        task: law.Task,
        events: ak.Array,
        models: list[Any],
        fold_indices: ak.Array,
        events_used_in_training: bool = False,
    ) -> ak.Array:
        # fake evaluation

        events = set_ak_column(events, f"{self.cls_name}.n_muon", 1)
        events = set_ak_column(events, f"{self.cls_name}.n_electron", 1)

        return events

```
The evaluations output is saved as parquet file with the name of the model as field name.
To get the files path afterwards, rerun the same law command again, but with `--print-output 0` at the end.

# Step 2: Initialization of the machine learning model
After defining all ABC function in Step 1, the model needs to be initialized with a unique `name`.
The model gets access to hyper parameter settings by deriving from a configuration dictionary.
All items of the dictionary are accessible as attribute from within the model.
```{literalinclude} ./ml_code.py
:language: python
:start-at: # create configuration dictionaries
:end-at: test_model = TestModel.derive(
```

REMOVE
```python
# create configuration dictionaries
hyperparameters = {
    "folds": 3,
    "epochs": 5,
}

datasets = {"datasets_name":[
    "hh_ggf_bbtautau_madgraph",
    "tt_sl_powheg",
    ]
}

configuration_dict = {"features": ("n_jet", "n_electron")}

configuration_dict.update(datasets)
configuration_dict.update(hyperparameters)

# init model instance with config dictionary
test_model = TestModel.derive("test_model", cls_dict=configuration_dict)

```
After the initialization, `law` needs to be informed in the `law.cfg` about the existence of the new machine learning model.
Add in the `law.cfg`, in the section of your `analysis`, the path to the Python file where the model's definition and initialization are done to the entries: `ml_modules`, `inference_modules`. (TODO what exactly does these modules do?)
The given path is relative to the analysis root directory.
```bash
[analysis]
# any other entries

ml_modules: hbt.ml.{test}
inference_modules: hbt.inference.test
```
# Commands to start training and evaluation
To start the machine learning training `law run cf.MLTraining`, while for evaluation use `law run cf.MLEvaluation`.
When not using the `config`, `calibrators`, `selector`s or `dataset`
```bash
# run training
law run cf.MLTraining \
    --version your_current_version \
    --ml-model test_model_name \
    --config run2_2017_nano_uhh_v11_limited \
    --calibrators calibrator_name \
    --selector selector_name \
    --dataset datasets_1,dataset_2,dataset_3,...

law run cf.MLEvaluation \
    --version v1 \
    --ml-model test_model \
    --config run2_2017_nano_uhh_v11_limited \
    --calibrators skip_jecunc

```
Most of these settings should sound familiar, if not look into the corresponding tutorial.
`version` defines a setup configuration of your ML task, think more of a label than of an actual `version`.
If you change the label, you

# Optional useful functions:
## Separate training and evaluation configuration for configs, calibrators, selector and producers:
By default chosen configs, calibrators, selector and producer are used for both training and evaluation.
Sometimes one do not want to share the same environment or does not need all the columns in evaluation as in training, this is where a separation of both comes in handy.

To separate this behavior one need to define the training_{configs,calibrators, selector,producers}.
These function take always the `config_inst` as first, and the requested_{configs,calibrators, selector,producers} as second parameter.
If this function is defined the evaluation will use the give `config`,`calibrator`, `selector` or `producer`, while the training will use one defined in the function.

In the following case, training will use a fixed selector and producer called default, while the calibrator uses an own defined calibrator called "skip_jecunc":

```{literalinclude} ./ml_code.py
:language: python
:start-at: def training_selector
:end-at: return ["default"]
```
```{literalinclude} ./ml_code.py
:language: python
:start-at: def training_producers
:end-at: return ["default"]
```
```{literalinclude} ./ml_code.py
:language: python
:start-at: def training_calibrators
:end-at: return ["skip_jecunc"]
```

REMOVE
```python

    def training_selector(self, config_inst: od.Config, requested_selector: str) -> str:
        # training uses the default selector
        return "default"

    def training_producers(self, config_inst: od.Config, requested_producers: Sequence[str]) -> list[str]:
        # training uses the default producer
        return ["default"]

    def training_calibrators(self, config_inst: od.Config, requested_calibrators: Sequence[str]) -> list[str]:
        # training uses a calibrator named "skip_jecunc"
        return ["skip_jecunc"]

```
## setup
Setup is called at the end of `__init__` of your ML model.
Within this function you can prepare operations that should stay open during the whole life time of your instance.
An typical example for this would the opening of a file or dynamic appending of variables that are only rel event for your ML model, and no where else.
In the following example plotting variables are added to the ML model:


```{literalinclude} ./ml_code.py
:language: python
:start-at: def setup
:end-at: return
```


```python
def setup(self):
    # dynamically add variables for the quantities produced by this model
    if f"{self.cls_name}.n_muon" not in self.config_inst.variables:
        self.config_inst.add_variable(
            name=f"{self.cls_name}.n_muon",
            null_value=-1,
            binning=(4, -1.5, 2.5),
            x_title="Predicted number of muons",
        )
        self.config_inst.add_variable(
            name=f"{self.cls_name}.n_electron",
            null_value=-1,
            binning=(4, -1.5, 2.5),
            x_title="Predicted number of electrons",
        )
```
