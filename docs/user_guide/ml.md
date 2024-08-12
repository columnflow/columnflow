# Machine Learning

In this section, the users will learn how to implement machine learning in their analysis with columnflow.

## How training happens in Columnflow: K-fold cross validation

Machine learning in columnflow is implemented in a way that k-fold cross validation is enabled by default.
In k-fold cross validation, the dataset is split in k-parts of equal size.
For each training, k-1 parts are used for the actual training and the remaining part is used to test the model.
This process is repeated k-times, resulting in the training of k-model instances.
After the training, columnflow will save all k-models, these can then be used for evaluation.
An overview and further details about possible variations of k-fold cross validation can be found in the [sci-kit documentation](https://scikit-learn.org/stable/modules/cross_validation.html).

## Configure your custom machine learning class:

To create a custom machine learning (ML) class in columnflow, it is imperative to inherit from the {py:class}`~columnflow.ml.MLModel` class.
This inheritance ensures the availability of functions to manage and access config and model instances, as well as the necessary producers.
The name of your custom ML class can be arbitrary, since `law` accesses your machine learning model using a `cls_name` in {py:meth}`~columnflow.util.DerivableMeta.derive`, e.g.

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: test_model
:end-at: test_model
```

The second argument in {py:meth}`~columnflow.util.DerivableMeta.derive` is a `cls_dict` which configures your subclass.
The `cls_dict` needs to be flat.
The keys of the dictionary are set as class attributes and are therefore also accessible by using `self`.
The configuration with `derive` has two main advantages:

- **manageability**, as the dictionary can come from loading a config file and these can be changed easily
- **flexibility**, multiple settings require only different configuration files

A possible configuration and model initialization for such a model could look like this:

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: hyperparameters =
:end-at: test_model = TestModel.derive(
```

One can also define class variables within the model.
This is can be useful for attributes that don't change often, for example to define the datasets your model uses.
Since these are also class attributes, they are accessible by using `self` and are also shared between all instances of this model.

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: class TestModel(
:end-at: your instance variables
```

If you have settings that should not be shared between all instances, define them within `__init__`.

## After the configuration

After the configuration of your ML model, `law` needs to be informed in the `law.cfg` about the existence of the new machine learning model.
Add in your `law.cfg`, under the section `analysis`, a `ml_modules` keyword, where you point to the Python module where the model's definition and derivation happens.
These import structures are relative to the analysis root directory.

```bash
[analysis]
# any other config entries

# if you want to include multiple things from the same parent module, you can use a comma-separated list in {}
ml_modules: hbt.ml.{test}
inference_modules: hbt.inference.test
```

## ABC functions

In the following we will go through several abstract functions that you must overwrite, in order to be able to use your custom ML class with columnflow.

### sandbox:

In {py:meth}`~columnflow.ml.MLModel.sandbox`, you specify which sandbox setup file should be sourced to setup the environment for ML usage.
The return value of {py:meth}`~columnflow.ml.MLModel.sandbox` is the path to your shell (.sh) file, e.g:

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: def sandbox
:end-at: return "bash::$HBT_BASE/sandboxes/venv_columnar_tf.sh"
```

It is recommended to start the path with `bash::`, to indicate that you want to source the {py:meth}`~columnflow.ml.MLModel.sandbox` with `bash`.
How to actually write the setup and requirement files can be found in the section about [setting up a sandbox](building_blocks/sandbox).

### datasets:

In the {py:meth}`~columnflow.ml.MLModel.datasets` function, you specify which datasets are important for your machine learning model and which dataset instance(s) should be extracted from your config.
To use this function, your datasets needs to be added to your campaign, as defined by the [Order](https://python-order.readthedocs.io/en/latest/) Module.
An example can be found [here](https://github.com/uhh-cms/cmsdb/blob/d83fb085d6e43fe9fc362df75bbc12816b68d413/cmsdb/campaigns/run2_2018_nano_uhh_v11/top.py).
It is recommended to return this as `set`, to prevent double counting.
In the following example all datasets given by the {ref}`external config <init_of_ml_model>` are taken, but also an additional dataset is given.
Note how the name of each dataset is used to get a `dataset instance` from your `config instance`.
This ensures that you properly use the correct dataset.

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: def datasets
:end-at: return set(dataset_inst)
```

### produces:

By default, intermediate columns are not saved within the columnflow framework but are filtered out afterwards.
If you want to prevent certain columns from being filtered out, you need to inform columnflow.
This is done by letting {py:meth}`~columnflow.ml.MLModel.produces` return the names of all columns that should be preserved.
The names of the columns should be returned as strings within an iterable.
More information can be found in the official documentation about [producers](building_blocks/producers).

In the following example, I want to tell columnflow to preserve the output of the neural network as well as the fold indices, to distinguish the different models with their corresponding training and test sets.
I do not store the input and target columns of each fold in order to save disk space, as they are already stored in the parquet file used for training and the same information should not be saved several times.
To avoid confusion, we do not produce the columns in this function, we only tell columnflow to not throw them away.

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: def produces
:end-at: return preserved_columns
```

Thus, whether to include these columns or not is your choice.

### uses:

In `uses` you define the columns that are needed by your machine learning model, and are forwarded to the ML model during the execution of the various tasks.
In this case we want to request the input and target features, as well as some weights:

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: def uses(
:end-at: return columns
```

### output:

In the {py:meth}`~columnflow.ml.MLModel.output` function, you define the local target directory that your current model instance will have access to.

Since machine learning in columnflow uses k-fold cross validation by default, it is a good idea to have a separate directory for each fold, and this should be reflected in the {py:meth}`~columnflow.ml.MLModel.output` path.
It is of good practice to store your "machine-learning-instance" files within the directory of the model instance.
To get the path to this directory use `task.target`.
In this example we want to save each fold separately e.g:

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: def output
:end-at: return target
```

### open_model:

In the {py:meth}`~columnflow.ml.MLModel.open_model` function, you implement the loading of the trained model and if necessary its configuration, so it is ready to use for {py:meth}`~columnflow.ml.MLModel.evaluate`.
This does not define how the model is build for {py:meth}`~columnflow.ml.MLModel.train`.

The `target` parameter represents the local path to the models directory.
In the following example a TensorFlow model saved with Keras API is loaded and returned, and no further configuration happens:

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: def open_model
:end-at: return loaded_model
```

### train:

In the {py:meth}`~columnflow.ml.MLModel.train` function, you implement the initialization of your models and the training loop.
The `task` corresponding to the model training is {py:class}`~columnflow.tasks.ml.MLTraining`.
By default {py:meth}`~columnflow.ml.MLModel.train` has access to the location of the models' `inputs` and `outputs`.

In columnflow, k-fold cross validation is enabled by default.
The `self` argument in {py:meth}`~columnflow.ml.MLModel.train` refers to the instance of the fold.
Using `self`, you have also access to the entire `analysis_inst`ance, the `config_inst`ance of the current fold, and to all the derived parameters of your model.

With this information, you can call and prepare the columns to be used by the model for training.
In the following example a very simple dummy training loop is performed using the Keras fit function.
Within this function some helper functions are used that are further explained in the following chapter about good practices.

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: def train
:end-at: return
```

#### Good practice for training:

It is considered to be a good practice to define helper functions for model building and column preprocessing and call them in {py:meth}`~columnflow.ml.MLModel.train`.
The reasoning behind this design decision is that in {py:meth}`~columnflow.ml.MLModel.train`, you want to focus more on the definition of the actual trainings loop.
Another reason is that especially the preparation of events can take a large amount of code lines, making it messy to debug.
The following examples show how to define these helper functions.
First of all, one needs a function to handle the opening and combination of all parquet files.

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: def open_input_files
:end-at: return all_events
```

In `prepare_events` we use the combined awkward array and filter out all columns we are not interested in during the training, to stay lightweight.
Next we split the remaining columns into input and target column and bring these columns into the correct shape, data type, and also use certain preprocessing transformations.
At the end, we transform the awkward array into a Tensorflow tensor, something that our machine learning model can handle.

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: def prepare_events
:end-at: return tf.convert_to_tensor
```

The actual building of the model is also handled by a separate function.

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: def build_model
:end-at: return model
```

### evaluate:

While the training process of the ML model was defined in {py:meth}`~columnflow.ml.MLModel.train`, its evaluation is defined in {py:meth}`~columnflow.ml.MLModel.evaluate`.
The corresponding `task` is {py:class}`~columnflow.tasks.ml.MLEvaluation`, which requires {py:class}`~columnflow.tasks.ml.MLTraining` and will therefore trigger a training if no training was performed before.

For each fold of the k-folds, a neural network model is trained and can be accessed through the `models` parameter.
The actual loading of the trained model stored in the `models` list is defined in the {py:meth}`~columnflow.ml.MLModel.open_model` function.

The awkward array `events` is loaded in chunks and contains the merged events of all folds.
To filter out the test set, create a mask with `fold_indices`.

If you want to preserve columns and write them out into a parquet file, append the columns to `events` using {py:func}`~columnflow.columnar_util.set_ak_column` and return `events`.
All columns not present in {py:meth}`~columnflow.ml.MLModel.produces` are then filtered out.

In the following example, the models prediction as well as the number of muons and electrons are saved, all the other columns in events are thrown away, since they are not present in `produce`:

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: def evaluate
:end-at: return events
```

Do not confuse this behavior with the parameter `events_used_in_training`.
This flag determines if a certain `dataset` and shift combination can be used by the task.

The evaluations output is saved as parquet file with the name of the model as field name.
To get the files path afterwards, rerun the same law command again, but with `--print-output 0` at the end.

## Commands to start training and evaluation

To start the machine learning training `law run cf.MLTraining`, while for evaluation use `law run cf.MLEvaluation`.

```bash
law run cf.MLTraining \
    --version your_current_version \
    --ml-model test_model_name \
    --config run2_2017_nano_uhh_v11_limited \
    --calibrators calibrator_name \
    --selector selector_name \
    --dataset datasets_1,dataset_2,dataset_3,...
```

```bash
law run cf.MLEvaluation \
    --version your_current_version \
    --ml-model test_model_name \
    --config run2_2017_nano_uhh_v11_limited \
    --calibrators calibrator_name
```

Most of these settings should sound familiar, and if not, please look into the corresponding tutorial.
`version` defines a setup configuration of your ML task, think more of a label than of an actual `version`.
If you change the version label, columnflow will rerun all dependencies that are unique for this label, this typically just means you will retrain a new model.
You can then switch freely between both models version with the `--cf.MLTraining-version`.

## Optional useful functions

### Separate training and evaluation configuration for configs, calibrators, selector and producers

The chosen configs, calibrators, selector and producer are used for both training and evaluation.
However, sometimes, one does not want to share the same environment or does not need the same columns in evaluation and in training.
Another possible scenario is the usage of different selectors or datasets, to be able to explore different phase spaces.
This is where a separation of both comes in handy.

To separate this behavior one need to define the training_{configs,calibrators, selector,producers}.
These functions take always the `config_inst` as first, and the requested_{configs,calibrators,selector,producers} as second parameter.
If this function is defined the evaluation will use the externally-defined `config`,`calibrator`, `selector` or `producer`, while the training will use the one defined in the function.

In the following case, training will use a fixed selector and producer called `default`, and a custom-defined calibrator called `skip_jecunc`:
the calibrator for the evaluation is provided by the used command.
Take special note on the numerus of the functions name and of course of the type hint.
The selector expects only a string, since we typically apply only 1 selection, while the calibrator or producers expect a sequence of strings.
In this special case we use an own defined calibrator called "skip_jecunc", which is of course linked within the law.cfg.

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: def training_selector
:end-at: return "default"
```

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: def training_producers
:end-at: return ["default"]
```

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: def training_calibrators
:end-at: return ["skip_jecunc"]
```

### setup

Setup is called at the end of `__init__` of your ML model.
Within this function you can prepare operations that should stay open during the whole lifetime of your instance.
A typical example for this would be the opening of a file or dynamically appending variables that are only relevant to your ML model, and nowhere else.
In the following example definitions needed to plot variables that are created in ML model are added to the `config_inst`:

```{literalinclude} ./examples/ml_code.py
:language: python
:start-at: def setup
:end-at: return
```

## Adjusting the ML model settings via the command line

In the configuration of your ML model, it can be helpful to define the `self.parameters` attribute, which stores all relevant network parameters in the following way:

```python
    self.parameters = {
        "parameter_name": self.parameters.get("parameter_name", default_value),
        "batchsize": int(self.parameters.get("batchsize", 1024)),
        ...
    }
```

With that, the model's parameters can be changed on the command line via the `--ml-model-settings` parameter.

```shell
law run cf.MLTraining --version v1 --ml-model MyModel --ml-model-settings "batchsize=2048,layers=32;32;32,some_other_parameter=0.7"
```

The task can parse the user's input of the `--ml-model-settings` parameter and can write the key-value pairs directly into the `self.parameters` attribute.

:::{dropdown} Example of the ML Model

```python
class MyModel(MLModel):
    def __init__(
        self,
        *args,
        **kwargs,
    ):
        # we cannot cast to dict on command line, but one can do this by hand
        ml_process_weights = self.parameters.get("ml_process_weights", {"st": 1, "tt": 2})
        if isinstance(ml_process_weight, tuple):
            ml_process_weights = {proc: int(weight) for proc, weight in [s.split(":") for s in ml_process_weights]}

        # store parameters of interest in the ml_model_inst, e.g. via the parameters attribute
        self.parameters = {
            "batchsize": int(self.parameters.get("batchsize", 1024)),
            "layers": tuple(int(layer) for layer in self.parameters.get("layers, (64, 64, 64)),
            "ml_process_weights": ml_process_weights
        }

        # create representation of ml_model_inst
        self.parameters_repr = law.util.create_hash(sorted(self.parameters.items()))

```

```shell
law run cf.MLTraining --version v1 --ml-model MyModel --ml-model-settings "batchsize=2048,layers=32;32;32,ml_process_weights=st:1;tt:4"
```

:::

:::{dropdown} Limitations of this parameter
At the moment, the `ml_model_settings` is not implemented for the `MLModelsMixin`, therefore we can only use the "default" model (or create new model via `derive`) when e.g. creating histograms.
:::
