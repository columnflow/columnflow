# coding: utf-8

"""
Test model definition.
"""

from __future__ import annotations

import law
import order as od

from columnflow.types import Any
from columnflow.ml import MLModel
from columnflow.util import maybe_import, dev_sandbox
from columnflow.columnar_util import Route, set_ak_column

ak = maybe_import("awkward")
tf = maybe_import("tensorflow")

law.contrib.load("tensorflow")


class ExampleModel(MLModel):

    # mark the model as accepting only a single config
    single_config = True

    def setup(self):
        # dynamically add variables for the quantities produced by this model
        if f"{self.cls_name}.output" not in self.config_inst.variables:
            self.config_inst.add_variable(
                name=f"{self.cls_name}.output",
                null_value=-1,
                binning=(20, -1.0, 1.0),
                x_title=f"{self.cls_name} DNN output",
            )

    def sandbox(self, task: law.Task) -> str:
        return dev_sandbox("bash::$__cf_short_name_uc___BASE/sandboxes/example.sh")

    def datasets(self, config_inst: od.Config) -> set[od.Dataset]:
        return {
            config_inst.get_dataset("st_tchannel_t_powheg"),
            config_inst.get_dataset("tt_sl_powheg"),
        }

    def uses(self, config_inst: od.Config) -> set[Route | str]:
        return {
            "Jet.pt", "Muon.pt",
        }

    def produces(self, config_inst: od.Config) -> set[Route | str]:
        return {
            f"{self.cls_name}.ouptut",
        }

    def output(self, task: law.Task) -> law.FileSystemDirectoryTarget:
        return task.target(f"mlmodel_f{task.branch}of{self.folds}", dir=True)

    def open_model(self, target: law.FileSystemDirectoryTarget) -> tf.keras.models.Model:
        return target.load(formatter="tf_keras_model")

    def train(
        self,
        task: law.Task,
        input: dict[str, list[dict[str, law.FileSystemFileTarget]]],
        output: law.FileSystemDirectoryTarget,
    ) -> None:
        # define a dummy NN
        x = tf.keras.Input(shape=(2,))
        a1 = tf.keras.layers.Dense(10, activation="elu")(x)
        y = tf.keras.layers.Dense(2, activation="softmax")(a1)
        model = tf.keras.Model(inputs=x, outputs=y)

        # the output is just a single directory target
        output.dump(model, formatter="tf_keras_model")

    def evaluate(
        self,
        task: law.Task,
        events: ak.Array,
        models: list[Any],
        fold_indices: ak.Array,
        events_used_in_training: bool = False,
    ) -> ak.Array:
        # fake evaluation
        events = set_ak_column(events, f"{self.cls_name}.output", 0.5)

        return events


# usable derivations
example = ExampleModel.derive("example", cls_dict={"folds": 2})
