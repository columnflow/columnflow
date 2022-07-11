# coding: utf-8

"""
Dummy training script.
"""

from typing import List, Any, Set, Union

import law
import order as od

from ap.ml import MLModel
from ap.util import maybe_import, dev_sandbox, FunctionArgs
from ap.columnar_util import Route, set_ak_column

ak = maybe_import("awkward")
tf = maybe_import("tensorflow")


class TestModel(MLModel):

    def set_config(self, *args, **kwargs):
        super().set_config(*args, **kwargs)

        # dynamically add variables for the quantities produced by this model
        if f"{self.name}.n_muon" not in self.config_inst.variables:
            self.config_inst.add_variable(
                name=f"{self.name}.n_muon",
                null_value=-1,
                binning=(4, -1.5, 2.5),
                x_title="Predicted number of muons",
            )
            self.config_inst.add_variable(
                name=f"{self.name}.n_electron",
                null_value=-1,
                binning=(4, -1.5, 2.5),
                x_title="Predicted number of electrons",
            )

    def sandbox(self, task: law.Task) -> str:
        return dev_sandbox("bash::$AP_BASE/sandboxes/venv_ml_tf.sh")

    def datasets(self) -> Set[od.Dataset]:
        return {
            self.config_inst.get_dataset("st_tchannel_t"),
            self.config_inst.get_dataset("tt_sl"),
        }

    def uses(self) -> Set[Union[Route, str]]:
        return {"ht", "n_jet", "n_muon", "n_electron", "normalization_weight"}

    def produces(self) -> Set[Union[Route, str]]:
        return {f"{self.name}.n_muon", f"{self.name}.n_electron"}

    def output(self, task: law.Task) -> FunctionArgs:
        return FunctionArgs(f"mlmodel_f{task.fold}of{self.folds}", dir=True)

    def open_model(self, target: law.LocalDirectoryTarget) -> tf.keras.models.Model:
        return tf.keras.models.load_model(target.path)

    def train(
        self,
        task: law.Task,
        input: Any,
        output: law.LocalDirectoryTarget,
    ) -> None:
        # define a dummy NN
        x = tf.keras.Input(shape=(2,))
        a1 = tf.keras.layers.Dense(10, activation="elu")(x)
        y = tf.keras.layers.Dense(2, activation="softmax")(a1)
        model = tf.keras.Model(inputs=x, outputs=y)

        # the output is just a single directory target
        output.parent.touch()
        model.save(output.path)

    def evaluate(
        self,
        task: law.Task,
        events: ak.Array,
        models: List[Any],
        fold_indices: ak.Array,
        events_used_in_training: bool = False,
    ) -> None:
        # fake evaluation
        set_ak_column(events, f"{self.name}.n_muon", 1)
        set_ak_column(events, f"{self.name}.n_electron", 1)


# pre-register models
TestModel.new("test_model", folds=3)
