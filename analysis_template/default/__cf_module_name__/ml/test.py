# coding: utf-8

"""
Test model definition.
"""

from __future__ import annotations

from typing import Any

import law
import order as od

from columnflow.ml import MLModel
from columnflow.util import maybe_import, dev_sandbox
from columnflow.columnar_util import Route, set_ak_column


ak = maybe_import("awkward")
tf = maybe_import("tensorflow")

law.contrib.load("tensorflow")


class TestModel(MLModel):

    def __init__(self, *args, folds: int | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        # class-to-instance-level attributes
        # (before being set, self.folds refers to a class-level attribute)
        self.folds = folds or self.folds

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

    def sandbox(self, task: law.Task) -> str:
        return dev_sandbox("bash::$HBT_BASE/sandboxes/venv_columnar_tf.sh")

    def datasets(self) -> set[od.Dataset]:
        return {
            self.config_inst.get_dataset("hh_ggf_bbtautau_madgraph"),
            self.config_inst.get_dataset("tt_sl_powheg"),
        }

    def uses(self) -> set[Route | str]:
        return {
            "ht", "n_jet", "n_muon", "n_electron", "normalization_weight",
        }

    def produces(self) -> set[Route | str]:
        return {
            f"{self.cls_name}.n_muon", f"{self.cls_name}.n_electron",
        }

    def output(self, task: law.Task) -> law.FileSystemDirectoryTarget:
        return task.target(f"mlmodel_f{task.fold}of{self.folds}", dir=True)

    def open_model(self, target: law.FileSystemDirectoryTarget) -> tf.keras.models.Model:
        return target.load(formatter="tf_keras_model")

    def train(
        self,
        task: law.Task,
        input: dict[str, list[law.FileSystemFileTarget]],
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
        events = set_ak_column(events, f"{self.cls_name}.n_muon", 1)
        events = set_ak_column(events, f"{self.cls_name}.n_electron", 1)

        return events


# usable derivations
test_model = TestModel.derive("test_model", cls_dict={"folds": 3})
