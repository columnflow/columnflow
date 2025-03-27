from __future__ import annotations
from typing import Callable
from collections.abc import Collection, Sequence
from dataclasses import asdict

import law.util
import order as od

from columnflow.production import Producer
from columnflow.weight import WeightProducer
from columnflow.util import maybe_import

hist = maybe_import("hist")
Hist = hist.Hist
np = maybe_import("numpy")

logger = law.logger.get_logger(__name__)


def reduce_hist(
    hist: Hist,
    reduce: str | Collection[str] | dict[str, sum | int] | Ellipsis = Ellipsis,
    exclude: str | Collection[str] = tuple(),
):
    exclude = law.util.make_list(exclude)
    if reduce is Ellipsis:
        hist = hist.project(*exclude)
    elif reduce is not None:
        if not isinstance(reduce, dict):
            reduce = law.util.make_list(reduce)
        hist = hist[{
            v: (reduce[v] if isinstance(reduce, dict) else sum)
            for v in reduce
            if v not in exclude}
        ]
    return hist


def loop_hists(*hists: Hist, exclude_axes: str | Collection[str] = tuple()) -> tuple[dict[str, int], list[Hist]]:
    assert all([h.axes == hists[0].axes for h in hists]), "axes are incompatible"

    axes = [ax for ax in hists[0].axes if ax.name not in law.util.make_list(exclude_axes)]
    for idx in np.ndindex(*map(len, axes)):
        idx = {ax.name: i for ax, i in zip(axes, idx)}
        yield idx, [h[idx] for h in hists] if len(hists) > 1 else hists[0][idx]


def collect_hist(histograms: dict[od.Dataset, Hist]) -> dict[str, Hist]:
    # sum all mc / data histograms
    summed_histograms = {}
    for p in ["mc", "data"]:
        summed_histograms[p] = sum([histograms[dt] for dt in histograms if getattr(dt, f"is_{p}")])
        assert summed_histograms[p], f"did not find any {p} histograms"
    return summed_histograms


def syst_hist(
    axes: Sequence[hist.axis.AxisProtocol],
    syst_name: str = "",
    arrays: np.ndarray | tuple[np.ndarray, np.ndarray] = None,
) -> Hist:
    if syst_name == "central":
        variations = [syst_name]
    else:
        variations = [f"{syst_name}_{dr}" if syst_name else dr for dr in [od.Shift.DOWN, od.Shift.UP]]

    h = Hist(
        hist.axis.StrCategory(variations, name="systematic", growth=True),
        *[ax for ax in axes if ax.name != "systematic"],
        storage=hist.storage.Weight,
    )
    h.variances()[:] = 1  # plotting functions assume presence of variances
    if arrays is not None:
        h.values()[:] = np.array(arrays)
    return h


def calculate_efficiency(
    histogram: Hist,
    trigger: str,
    reference: str,
    eff_func: Callable[[Hist, Hist], Hist] = lambda sel, tot: sel / tot.values(),
):
    # counts that pass both triggers
    selected_counts = histogram[{reference: 1, trigger: 1}]
    # counts that pass reference triggers
    incl = histogram[{reference: 1, trigger: sum}]
    # calculate efficiency
    return eff_func(selected_counts, incl)


def correlation_efficiency_bias(passfailhist):
    """
    calculate efficiecy bias of trigger T w.r.t. reference trigger M based on frequency matrix.
    The bias is defined as Eff(T|M=1) / Eff(T|M=0)

    Returns: bias, bias - 1sig error, bias + 1sig error

    """
    import statsmodels.genmod.generalized_linear_model as glm
    from statsmodels.genmod.families import Binomial
    from statsmodels.genmod.families.links import Log
    from statsmodels.tools.sm_exceptions import DomainWarning

    total = passfailhist[sum, sum]
    passfailmatrix = passfailhist.values() * total.value / total.variance

    assert passfailmatrix.shape == (2, 2), "invalid pass-fail-matrix"

    if np.any(np.sum(np.abs(passfailmatrix), axis=0) < 1e-9):
        return (np.nan,) * 2

    if np.any(passfailmatrix < 0):
        logger.warning("Pass-Fail matrix has negative counts. Negative counts are set to 0.")
        passfailmatrix[passfailmatrix < 0] = 0

    trigger_values = np.array([0, 0, 1, 1])
    ref_trigger_values = np.array([0, 1, 0, 1])

    # log Eff(T|M) = b + a * M
    # Eff(T|M = 1) / Eff(T|M = 0) = exp(a)
    with np.testing.suppress_warnings() as sup:
        # Using the Log linking function with binomial values is convenient,
        # but can result in issues since the function does not map to [0, 1].
        # To avoid constant warnings about this, they are suppressed here
        sup.filter(DomainWarning)
        model = glm.GLM(trigger_values,
                        np.transpose([[1, 1, 1, 1],
                                      ref_trigger_values]),
                        freq_weights=passfailmatrix.flatten(),
                        family=Binomial(link=Log()))
        res = model.fit()

    # multiplicative bias = exp(a)
    mult_bias = np.exp(res.params[1])
    # d(exp a) = exp(a) da
    var = res.cov_params()[1, 1] * mult_bias ** 2

    return mult_bias - 1, var


def init_trigger_config(self: Producer | WeightProducer):
    if callable(self.trigger_config):
        self.trigger_config = self.trigger_config()

    for key, value in asdict(self.trigger_config).items():
        if not hasattr(self, key):
            setattr(self, key, value)


def init_uses_variables(self: Producer | WeightProducer):
    self.uses.update({
        inp
        for variable_inst in self.variables
        for inp in (
            [variable_inst.expression] if isinstance(variable_inst.expression, str) else variable_inst.x("inputs",
                                                                                                         [])
        )
    })
