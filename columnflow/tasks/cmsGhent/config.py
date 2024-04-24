import law
import luigi
from columnflow.tasks.framework.base import ConfigTask
from columnflow.tasks.framework.mixins import DatasetsProcessesMixin

import json


class ReadDataSets(DatasetsProcessesMixin, ConfigTask):

    shifts = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, print the shifted datasets, not the nominal",
    )

    def output(self) -> law.target.file.FileSystemFileTarget:
        """
        Creates a target file for the final .json file containing the list of datasets

        """
        return self.target(("shifts" if self.shifts else "nominal") + ".json")

    def complete(self):
        return self.output().exists()

    def run(self):
        process_dataset_map = {p: [] for p in self.processes}

        for dt in self.datasets:
            dt = self.config_inst.get_dataset(dt)
            datasets = []
            process = list(dt.processes)[0]
            for p in self.processes:
                p_inst = self.config_inst.get_process(p)
                if p_inst.has_process(process) or p_inst == process:
                    datasets = process_dataset_map[p]
            datasets_loc: dict = dt.info.copy()
            nominal = datasets_loc.pop('nominal')
            if self.shifts:
                for shift in datasets_loc.values():
                    datasets.extend(shift.keys)
            else:
                datasets.extend(nominal.keys)

        self.output().dump(process_dataset_map, indent=2)
