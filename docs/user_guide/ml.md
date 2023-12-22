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

# ABC functions
In following we will go through several abstract functions that you must overwrite, in order to be able to use your custom ML class with columnflow.

## sandbox:
In {py:meth}`~columnflow.ml.MLModel.sandbox`, you specify which sandbox setup file should be sourced to setup the environment for ML usage.
The return value of {py:meth}`~columnflow.ml.MLModel.sandbox` is the path to your shell (sh) file, e.g:
```{literalinclude} ./ml_code.py
:language: python
:start-at: def sandbox
:end-at: return "bash::$HBT_BASE/sandboxes/venv_columnar_tf.sh"
```
It is recommended to start the path with `bash::`, to indicate that you want to source the {py:meth}`~columnflow.ml.MLModel.sandbox` with `bash`.
How to actual write the setup and requirement file can be found in the section about [setting up a sandbox](building_blocks/sandbox).

## datasets:
In the {py:meth}`~columnflow.ml.MLModel.datasets` function, you specify which datasets are important for your machine learning model and which dataset instance should be extracted from your config.
To use this function your datasets needs to be added to your campaign, as defined by the [Order](https://python-order.readthedocs.io/en/latest/>) Module.
An example can be found [here](https://github.com/uhh-cms/cmsdb/blob/d83fb085d6e43fe9fc362df75bbc12816b68d413/cmsdb/campaigns/run2_2018_nano_uhh_v11/top.py).
It is recommended to return this as `set`, to prevent double counting.
In the following example all datasets given by the {ref} external config <init_of_ml_model> are taken, but also an additional is given.
Note how the name of each dataset is used to get an `dataset instance` from your `config instance`, this ensures that you properly use the correct dataset.
```{literalinclude} ./ml_code.py
:language: python
:start-at: def datasets
:end-at: return set(dataset_inst)
```


## produces:
By default, intermediate columns are not saved within the columnflow framework but are filtered out afterwards.
If you want to prevent certain columns from being filtered out, you need to tell columnflow by writing them in the {py:meth}`~columnflow.ml.MLModel.produces`function.
This is always done by writing an iterable containing the name of the producer as string.
More information can be found in the official documentation about [producers](building_blocks/producers).

In the following example, I want to preserve the number of muons and electrons, but we also want to preserve for example the output of the neural network.
To avoid confusion, we are not producing the columns in this function, we only tell columnflow to not throwing them away.
```{literalinclude} ./ml_code.py
:language: python
:start-at: def produces
:end-at: return preserved_columns
```

## uses:


## output:
In the {py:meth}`~columnflow.ml.MLModel.output` function, you define your local target directory that your current model instance will have access to.

Since machine learning in columnflow uses k-fold by default, it is a good idea to have a separate directory for each fold, and this should be reflected in the {py:meth}`~columnflow.ml.MLModel.output` path.
It is of good practice to use `task.target` to be that your "machine-learning-task" related files are saved in the same directory.
In this example we want to save each fold separately e.g:
```{literalinclude} ./ml_code.py
:language: python
:start-at: def output
:end-at: return loaded_model
```


## open_model:
In the {py:meth}`~columnflow.ml.MLModel.open_model` function, you implement the loading of the trained model and if necessary its configuration, so it is ready to use for {py:meth}`~columnflow.ml.MLModel.evaluate`.
This does not define how the model is build for {py:meth}`~columnflow.ml.MLModel.train`.

The `target` parameter represents the local path to the models directory.
In the the following example a TensorFlow model saved with Keras API is loaded and returned, and no further configuration happens:
```{literalinclude} ./ml_code.py
:language: python
:start-at: def open_model
:end-at: return loaded_model
```


## train:
In the {py:meth}`~columnflow.ml.MLModel.train` function, you implement the initialization of your models and the training loop.
The `task` corresponding to the models training is {py:class}`~columnflow.tasks.ml.MLTraining`.
By default {py:meth}`~columnflow.ml.MLModel.train` has access to the location of the models `inputs` and `outputs`.

In columnflow, k-fold is enabled by default.
The `Self` argument in {py:meth}`~columnflow.ml.MLModel.train` referes to the instance of the fold.
Using `Self`, you have also access to the entire `analysis_inst`ance the `config_inst`ance of the current fold, and to all the derived parameters of your model.

With this information, you can call and prepare the columns to be used by the model for training.
In the following exmaple a very simple dummy trainings loop is performed using Keras fit function.
```{literalinclude} ./ml_code.py
:language: python
:start-at: def train
:end-at: return
```
### Good practice for training:
It is considered to be a good practice to define helper functions for model building and column preprocessing and call them in {py:meth}`~columnflow.ml.MLModel.train`.
The reasoning behind this design decision is that in {py:meth}`~columnflow.ml.MLModel.train`, you want to focus more on the definition of the actual trainings loop.
Another reason is that especially the preparation of events, can take a large amount of code lines, making it messy to debug.
In the following example it is shown how to define these helper functions.
First of all one needs a function to handle the opening and combination of all parquet files.
```{literalinclude} ./ml_code.py
:language: python
:start-at: def open_input_files
:end-at: return all_events
```

In `prepare_events` we use the combined awkward and filter out all columns we are not interested in during the training, to stay lightweight.
Next we split the remaining columns into input and target column and bring these columns into the correct shape, data type, and also use certain preprocessing transformation.
At the end we transform the awkward array into a Tensorflow tensor, something that our machine learning model can handle.
```{literalinclude} ./ml_code.py
:language: python
:start-at: def prepare_events
:end-at: return tf.convert_to_tensor
```
The actual building of the model is also handled by a separate function.
```{literalinclude} ./ml_code.py
:language: python
:start-at: def build_model
:end-at: return model
```
With this little example one can see why it is of good practice to separate this process into small chunks.

## evaluate:
TODO EVALUATION CHANGED WITH new PR. This chapter needs to be modified.

Within {py:meth}`~columnflow.ml.MLModel.train` one defined the trainings process of the ML model, while in {py:meth}`~columnflow.ml.MLModel.evaluate` its evaluation is defined.
The corresponding `task` is {py:class}`~columnflow.tasks.ml.MLEvaluation` and depends on {py:class}`~columnflow.tasks.ml.MLTraining` and will therefore trigger a training if no training was performed before.

For each fold of the k-folds a neural network model is trained and can be accessed from `models`.
The actual loading, of the trained model stored in the list, is defined in {py:meth}`~columnflow.ml.MLModel.open_model` function.

The awkward array `events` is loaded in chunks and contains the merged events of all folds.
To filter out the test set, create a mask with `fold_indices`.

If you want to preserve columns and write them out into a parquet file, append the columns to `events` using `set_ak_column` and return `events`.
All columns not present in {py:meth}`~columnflow.ml.MLModel.produces` are then filtered out.

In the following example, the models prediction as well as the number of muons and electrons are saved, all the other columns in events are thrown away, since they are not present in `use`:

Don't confuse this behavior with the parameter `events_used_in_training`.
This flag determines if a certain `dataset` and shift combination can be used by the task.

```{literalinclude} ./ml_code.py
:language: python
:start-at: def evaluate
:end-at: return events
```

The evaluations output is saved as parquet file with the name of the model as field name.
To get the files path afterwards, rerun the same law command again, but with `--print-output 0` at the end.

(init_of_ml_model)=
# Step 2: Initialization of the machine learning model
After defining all ABC function in Step 1, the model needs to be initialized with a unique `name`.
The model gets access to hyper parameter settings by deriving from a configuration dictionary.
All items of the dictionary are accessible as attribute from within the model.

```{literalinclude} ./ml_code.py
:language: python
:start-at: hyperparameters =
:end-at: test_model = TestModel.derive(
```

After the initialization, `law` needs to be informed in the `law.cfg` about the existence of the new machine learning model.
Add in the `law.cfg`, in the section of your `analysis`, the path to the Python file where the model's definition and initialization are done to the entries: `ml_modules`, `inference_modules`. (TODO what exactly does these modules do?)
The given path is relative to the analysis root directory.
```bash
[analysis]
# any other config entries

ml_modules: hbt.ml.{test}
inference_modules: hbt.inference.test
```
# Commands to start training and evaluation
To start the machine learning training `law run cf.MLTraining`, while for evaluation use `law run cf.MLEvaluation`.
When not using the `config`, `calibrators`, `selector`s or `dataset`
```bash
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
## setup
Setup is called at the end of `__init__` of your ML model.
Within this function you can prepare operations that should stay open during the whole life time of your instance.
An typical example for this would the opening of a file or dynamic appending of variables that are only relevant to your ML model, and no where else.
In the following example plotting variables are added to the ML model:
```{literalinclude} ./ml_code.py
:language: python
:start-at: def setup
:end-at: return
```
