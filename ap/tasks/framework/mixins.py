# coding: utf-8

"""
Lightweight mixins task classes.
"""

import law
import luigi

from ap.tasks.framework.base import AnalysisTask


class CalibratorMixin(AnalysisTask):

    calibrator = luigi.Parameter(
        default="test",
        description="the name of the calibrator to the applied; default: 'test'",
    )

    def store_parts(self):
        parts = super().store_parts()
        parts.insert_before("version", "calibrator", f"calib__{self.calibrator}")
        return parts


class CalibratorsMixin(AnalysisTask):

    calibrators = law.CSVParameter(
        default=("test",),
        description="comma-separated names of calibrators to be applied; default: 'test'",
    )

    def store_parts(self):
        parts = super().store_parts()

        calib = "__".join(self.calibrators[:5])
        if len(self.calibrators) > 5:
            calib += f"__{law.util.create_hash(self.calibrators[5:])}"
        parts.insert_before("version", "calibrators", f"calib__{calib}")

        return parts


class SelectorMixin(AnalysisTask):

    selector = luigi.Parameter(
        default="test",
        description="the name of the selector to the applied; default: 'test'",
    )

    def store_parts(self):
        parts = super().store_parts()
        parts.insert_before("version", "selector", f"select__{self.selector}")
        return parts


class CalibratorsSelectorMixin(SelectorMixin, CalibratorsMixin):
    pass


class ProducerMixin(AnalysisTask):

    producer = luigi.Parameter(
        default="test",
        description="the name of the producer to the applied; default: 'test'",
    )

    def store_parts(self):
        parts = super().store_parts()
        parts.insert_before("version", "producer", f"produce__{self.producer}")
        return parts


class ProducersMixin(AnalysisTask):

    producers = law.CSVParameter(
        default=("test",),
        description="comma-separated names of producers to be applied; default: 'test'",
    )

    def store_parts(self):
        parts = super().store_parts()

        produce = "__".join(self.producers[:5])
        if len(self.producers) > 5:
            produce += f"__{law.util.create_hash(self.producers[5:])}"
        parts.insert_before("version", "producers", f"produce__{produce}")

        return parts


class PlotMixin(AnalysisTask):

    view_cmd = luigi.Parameter(
        default=law.NO_STR,
        significant=False,
        description="a command to execute after the task has run to visualize plots right in the "
        "terminal; no default",
    )


@law.decorator.factory(accept_generator=True)
def view_output_plots(fn, opts, task, *args, **kwargs):
    def before_call():
        return None

    def call(state):
        return fn(task, *args, **kwargs)

    def after_call(state):
        view_cmd = getattr(task, "view_cmd", None)
        if not view_cmd or view_cmd == law.NO_STR:
            return

        # prepare the view command
        if "{}" not in view_cmd:
            view_cmd += " {}"

        # collect all paths to view
        view_paths = []
        outputs = law.util.flatten(task.output())
        while outputs:
            output = outputs.pop(0)
            if isinstance(output, law.TargetCollection):
                outputs.extend(output._flat_target_list)
                continue
            if not getattr(output, "path", None):
                continue
            if output.path.endswith((".pdf", ".png")) and output.path not in view_paths:
                view_paths.append(output.path)

        # loop through paths and view them
        for path in view_paths:
            task.publish_message("showing {}".format(path))
            law.util.interruptable_popen(view_cmd.format(path), shell=True, executable="/bin/bash")

    return before_call, call, after_call
