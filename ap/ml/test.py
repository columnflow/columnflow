# coding: utf-8

"""
Dummy training script.
"""

from ap.ml import MLModel
from ap.util import FunctionArgs
from ap.columnar_util import set_ak_column


class TestModel(MLModel):

    def define_datasets(self):
        return {
            self.config_inst.get_dataset("st_tchannel_t"),
            self.config_inst.get_dataset("tt_sl"),
        }

    def define_used_columns(self):
        return {"ht", "n_jet", "n_muon", "n_electron", "normalization_weight"}

    def define_produced_columns(self):
        return {f"{self.name}.n_muon", f"{self.name}.n_electron"}

    def define_output(self, task):
        return FunctionArgs(f"mlmodel_f{task.fold}of{self.folds}", dir=True)

    def open_model(self, output):
        import tensorflow as tf
        return tf.keras.models.load_model(output.path)

    def train(self, task, input, output):
        import tensorflow as tf

        # define a dummy NN
        x = tf.keras.Input(shape=(2,))
        a1 = tf.keras.layers.Dense(10, activation="elu")(x)
        y = tf.keras.layers.Dense(2, activation="softmax")(a1)
        model = tf.keras.Model(inputs=x, outputs=y)

        # the output is just a single directory target
        output.parent.touch()
        model.save(output.path)

    def evaluate(self, task, models, events, events_used_in_training=False):
        # dummy evaluation that does not need the models at all
        set_ak_column(events, f"{self.name}.n_muon", 1)
        set_ak_column(events, f"{self.name}.n_electron", 1)


# pre-register models
TestModel.new("test", folds=5)
