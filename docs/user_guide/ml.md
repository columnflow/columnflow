# Machine Learning

In this section, the users will learn how to implement machine learning in their analysis with
columnflow.

# General procedure:
In columnflow, the utilization of machine learning models involves a two-step process: defining a class and initialize an instance of that class.

# Step 1: Defining your custom machine learning class:
To create a machine learning class, it is imperative to inherit from the MLModel class. This inheritance ensures the availability of functions for managing and accessing config and model instances as well as the necessary producers.

The exact name of your class can be arbitrary, e.g:
```python
# define your model
class TestModel(MLModel):
    ...

```
Within your custom ML class, certain abstract functions must be defined by you to execute the code. These function include:
    - `sandbox`
    - `datasets`
    - `uses`
    - `produces`
    - `output`
    - `open_model`
    - `train`
    - `evaluate`
What these functions do and how to use them is elaborated on the subsequent section.

# ABC functions

## sandbox:
In the `sandbox`, you specify which `sandbox` setup file should be sourced to configure the environments.
The return value of `sandbox` is the path to your shell file, e.g:
```python
def sandbox(self, task: law.Task) -> str:
        return "bash::$CF_BASE/sandboxes/venv_ml_tf.sh"
```
To keep things organized, store your `sandbox` setup and requirement file in `$CF_BASE/sandboxes/`.
It is also recommended to start the path with `bash::`, to indicate that you want to source the `sandbox` with `bash`.

The content of the `sandbox` setup file is a modification of the `cf.sh`. So copy this file and modify the copy, by
change `export CF_VENV_REQUIREMENTS` to point to your `requirement.txt`, e.g:
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
Columnflow manages sandboxes by using a version number at very first line, styled as:`# version ANY_FLOAT`. This version defines a software packet, and it is good practice to change the version number, whenever environment is altered.

For more information, refer to the official [pip documentation](https://pip.pypa.io/en/stable/reference/requirements-file-format/).


## datasets:
In the `datasets` function, you specify which datasets are important for your machine learning model and which dataset instance should be extracted from your config. To use this function your datasets needs to be added to your campaign, as defined by the `Order` Module. An example can be found (here)[]. It is recommended to return this as `set`, to prevent double counting.
```python
def datasets(self, config_inst: od.Config) -> set[od.Dataset]:
    # passed to the model via config,
    # one can also define the dataset name here as list
    all_datasets_names = self.dataset_names

    # gather all dataset instance you are interested in
    dataset_inst = []
    for dataset_name in all_datasets:
        dataset_inst.append(config_inst.get_dataset(dataset_name))
return set(dataset_inst)
```

## produces:
By default, intermediate columns are not saved within the columnflow framework but are filtered out afterwards. If you want to prevent certain columns to be filtered out, you need to mark them. This is done in the `produces` function.
In the following the number of muons and electrons are preserved. For more information about producers look into [TODO]()
```python
    def produces(self, config_inst: od.Config) -> set[Route | str]:
        preseve_columns = {
            f"{self.cls_name}.n_muon",
            f"{self.cls_name}.n_electron",
        }
        return
```

## output:
In the `output`, you define your local target directory for your model current instance.

Since machine learning in columnflow uses k-fold by default, it is a good idea to have a separate directory for each fold, and this should be reflected in the output path. It is good practice to use `task.target` to have everything machine learning related in the same directory, e.g:
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

The `target` parameter represents the local path to the models directory. In the the following example a TensorFlow model saved with Keras API, along with its training history loaded and returned:
```python
    def open_model(self, target: law.LocalDirectoryTarget) -> tf.keras.models.Model:
        model = tf.keras.models.load_model(target.path)
        return model
```

## train:
In the `train` function, you implement the initialization of your models and the training loop.
By default train has access to the location of the models `inputs` and `outputs`.

In columnflow, k-fold is enabled by default. The `Self` argument in `train` referes to the instance of the fold. Using `Self`, you have access to the entire `analysis_inst`ance the `config_inst`ance of the current fold, and, of course, to all the derived parameters of your model.

With this information, you can call and prepare the columns to be used by the model for training.
It is considered good practice to define the model building and column preprocessing in functions outside of `train`, keeping to keep the definition of the trainings loop and the preparation apart. An example about how to prepare columns is can be found [here](TODO Link)

E.g. for a very simple trainings loop using Keras fit function.

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

            # best timing to save metrics, plots and so on...
```

## evaluate:
After a model is trained, it can be employed to evaluate new data. This evaluation is defined in the `evaluate` function.

It is important to note that `evaluate` function has a dependency on prior training. If training has not been executed yet, the function will initiate the training process. The loading of the trained model is implemented in `open_model` function.

In contrast to `train`ing, there is not need to specify the used columns explicitly, since all columns used in training are taken automatically. If you want to preserve columns and write them out, return the columns and add them to `produces`.

E.g. for an evaluation, where the models prediction is saved within the column:

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

# Step 2: Initialization of the machine learning model
After defining all ABC function in Step 1, the model needs to be initialized with a unique `name`. The model gets access to hyper parameter settings by deriving from a configuration dictionary. All items of the dictionary are accessible as attribute from within the model.
```python
# give the model access to several hyperparameters and extra information
hyperparameters = {
                "folds": 3,
                "epochs": 150,
                }

datasets = {"dataset_names":[
                "graviton_hh_ggf_bbtautau_m400_madgraph",
                "graviton_hh_vbf_bbtautau_m400_madgraph",
    ]
}

# init the model
test_model = TestModel.derive("test_model", cls_dict=**configuration_dict)

```

After the initialization, `law` needs to be informed in the `law.cfg` about the existence of the new machine learning model. In the `law.cfg`, in the section of your `analysis`, add the path to the Python file where the model's definition and initialization are done to the entries: `ml_modules`, `inference_modules`.
The path is relative to the analysis root directory.
```bash
[analysis]
# any other entries

ml_modules: hbt.ml.{test}
inference_modules: hbt.inference.test
```
# Commands to start training and evaluation


# Useful optional functions

## setup

## Separate configs, calibrators, selector and producers for training and evaluation:
By default chosen configs, calibrators, selector and producer are used for both training and evaluation.
Sometimes one do not want to share the same environment or does not need all the columns in evaluation as in training, this is where a separation of both comes in handy.

To separate this behavior one need to define the training_{configs,calibrators, selector,producers}. These function take always the `config_inst` as first, and the requested_{configs,calibrators, selector,producers} as second parameter.
If this function is defined the evaluation will use the give {configs,calibrators,selector,producers}, while the training will use the defined separated setting.

E.g. where a custom ... TODO is used:
```python

```
