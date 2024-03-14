# coding: utf-8

"""
Test model definition.
"""

from __future__ import annotations

from typing import Any, Sequence

import law
import order as od

from columnflow.ml import MLModel
from columnflow.util import maybe_import
from columnflow.columnar_util import Route, set_ak_column, remove_ak_column


ak = maybe_import("awkward")
tf = maybe_import("tensorflow")
np = maybe_import("numpy")


law.contrib.load("tensorflow")


class TestModel(MLModel):
    # shared between all model instances
    datasets: dict = {
        "datasets_name": [
            "hh_ggf_bbtautau_madgraph",
            "tt_sl_powheg",
        ],
    }

    def __init__(
            self,
            *args,
            folds: int | None = None,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        # your instance variables
        # these are exclusive to your model instance

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
        return

    def sandbox(self, task: law.Task) -> str:
        return "bash::$HBT_BASE/sandboxes/venv_columnar_tf.sh"

    def datasets(self, config_inst: od.Config) -> set[od.Dataset]:
        # normally you would pass this to the model via config and loop through these names ...
        all_datasets_names = self.datasets_name

        dataset_inst = []
        for dataset_name in all_datasets_names:
            dataset_inst.append(config_inst.get_dataset(dataset_name))

        # ... but you can also add one dataset by using its name
        dataset_inst.append(config_inst.get_dataset("tt_sl_powheg"))

        return set(dataset_inst)

    def uses(self, config_inst: od.Config) -> set[Route | str]:
        columns = set(self.input_features) | set(self.target_features) | {"normalization_weight"}
        return columns

    def produces(self, config_inst: od.Config) -> set[Route | str]:
        # mark columns that you don't want to be filtered out
        # preserve the networks prediction of a specific feature for each fold
        # cls_name would be the name of your model
        ml_predictions = {f"{self.cls_name}.fold{fold}.{feature}"
            for fold in range(self.folds)
            for feature in self.target_columns}

        # save indices used to create the folds
        util_columns = {f"{self.cls_name}.fold_indices"}

        # combine all columns to a unique set
        preserved_columns = ml_predictions | util_columns
        return preserved_columns

    def output(self, task: law.Task) -> law.FileSystemDirectoryTarget:
        # needs to be given via config
        max_folds = self.folds
        current_fold = task.fold

        # create directory at task.target, if it does not exist
        target = task.target(f"mlmodel_f{current_fold}of{max_folds}", dir=True)
        return target

    def open_model(self, target: law.FileSystemDirectoryTarget):
        # if a formatter exists use formatter
        # e.g. for keras models: target.load(formatter="tf_keras_model")
        loaded_model = tf.keras.models.load_model(target.path)
        return loaded_model

    def open_input_files(self, inputs):
        # contains files from all datasets
        events_of_datasets = inputs["events"][self.config_inst.name]

        # get datasets names
        # datasets = [dataset.label for dataset in self.datasets(self.config_inst)]

        # extract all columns from parquet files for all datasets and stack them
        all_events = []
        for dataset, parquet_file_targets in events_of_datasets.items():
            for parquet_file_target in parquet_file_targets:
                parquet_file_path = parquet_file_target["mlevents"].path
                events = ak.from_parquet(parquet_file_path)
                all_events.append(events)

        all_events = ak.concatenate(all_events)
        return all_events

    def prepare_events(self, events):
        # helper function to extract events and prepare them for training

        column_names = set(events.fields)
        input_features = set(self.input_features)
        target_features = set(self.target_features)

        # remove columns not used in training
        to_remove_columns = list(column_names - (input_features | target_features))

        for to_remove_column in to_remove_columns:
            print(f"removing column {to_remove_column}")
            events = remove_ak_column(events, to_remove_column)

        # ml model can't work with awkward arrays
        # we need to convert them to tf.tensors
        # this is done by following step chain:
        # ak.array -> change type to uniform type -> np.recarray -> np.array -> tf.tensor

        # change dtype to uniform type
        events = ak.values_astype(events, "float32")
        # split data in inputs and target
        input_columns = [events[input_column] for input_column in self.input_features]
        target_columns = [events[target_column] for target_column in self.target_features]

        # convert ak.array -> np.array -> bring in correct shape
        input_data = ak.concatenate(input_columns).to_numpy().reshape(
            len(self.input_features), -1).transpose()
        target_data = ak.concatenate(target_columns).to_numpy().reshape(
            len(self.target_features), -1).transpose()
        return tf.convert_to_tensor(input_data), tf.convert_to_tensor(target_data)

    def build_model(self):
        # helper function to handle model building
        x = tf.keras.Input(shape=(2,))
        a1 = tf.keras.layers.Dense(10, activation="elu")(x)
        y = tf.keras.layers.Dense(2, activation="softmax")(a1)
        model = tf.keras.Model(inputs=x, outputs=y)
        return model

    def train(
        self,
        task: law.Task,
        input: dict[str, list[law.FileSystemFileTarget]],
        output: law.FileSystemDirectoryTarget,
    ) -> None:

        # use helper functions to define model, open input parquet files and prepare events
        # init a model structure
        model = self.build_model()

        # get data tensors
        events = self.open_input_files(input)
        input_tensor, target_tensor = self.prepare_events(events)

        # setup everything needed for training
        optimizer = tf.keras.optimizers.SGD()
        model.compile(
            optimizer,
            loss="mse",
            steps_per_execution=10,
        )

        # train, throw model_history away
        _ = model.fit(
            input_tensor,
            target_tensor,
            epochs=5,
            steps_per_epoch=10,
            validation_split=0.25,
        )

        # save your model and everything you want to keep
        output.dump(model, formatter="tf_keras_model")
        return

    def evaluate(
        self,
        task: law.Task,
        events: ak.Array,
        models: list[Any],
        fold_indices: ak.Array,
        events_used_in_training: bool = False,
    ) -> ak.Array:
        # prepare ml_models input features, target is not important
        inputs_tensor, _ = self.prepare_events(events)

        # do evaluation on all data
        # one can get test_set of fold using: events[fold_indices == fold]
        for fold, model in enumerate(models):
            # convert tf.tensor -> np.array -> ak.array
            # to_regular is necessary to make array contigous (otherwise error)
            prediction = ak.to_regular(model(inputs_tensor).numpy())

            # update events with predictions, sliced by feature, and fold_indices for identification purpose
            for index_feature, target_feature in enumerate(self.target_features):
                events = set_ak_column(
                    events,
                    f"{self.cls_name}.fold{fold}.{target_feature}",
                    prediction[:, index_feature],
                )

        events = set_ak_column(
            events,
            f"{self.cls_name}.fold_indices",
            fold_indices,
        )

        return events

    def training_selector(
        self,
        config_inst: od.Config,
        requested_selector: str,
    ) -> str:
        # training uses the default selector
        return "default"

    def training_producers(
        self,
        config_inst: od.Config,
        requested_producers: Sequence[str],
    ) -> list[str]:
        # training uses the default producer
        return ["default"]

    def training_calibrators(
        self,
        config_inst: od.Config,
        requested_calibrators: Sequence[str],
    ) -> list[str]:
        # training uses a calibrator named "skip_jecunc"
        return ["skip_jecunc"]


# usable derivations
# create configuration dictionaries
hyperparameters = {
    "folds": 3,
    "epochs": 5,
}

# input and target features
configuration_dict = {
    "input_features": (
        "n_jet",
        "ht",
    ),
    "target_features": (
        "n_electron",
        "n_muon",
    ),
}

# combine configuration dictionary
configuration_dict.update(hyperparameters)

# init model instance with config dictionary
test_model = TestModel.derive(cls_name="test_model", cls_dict=configuration_dict)
