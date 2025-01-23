from __future__ import annotations
import warnings
import numpy as np
from scipy.stats import chi2
import scipy.optimize as opt
import matplotlib.pyplot as plt
import law

logger = law.logger.get_logger(__name__)


def prob1(x, m, y, n, th0):
    """
    maximal likelihood estimator for the efficiency p_1 = (x / m) assuming a fixed ratio
    p_1 / p_2 = (x / m) / (y / n) using in the Koopman criterion, where x and y are from two
    independent binomial distributions.

    Args:
        x: number of passes in first binomial variable
        m: sample size for first binomial variable
        y: number of passes in second binomial variable
        n: sample size for second binomial variable
        th0: fixed efficiency ratio p_1 / p_2

    Returns: p_1
    """
    t = th0 * (m + y) + (x + n)
    return (t - np.sqrt(t ** 2 - 4 * th0 * (m + n) * (x + y))) / (2 * (m + n))


def U(x, m, y, n, th0):
    """

    maximal likelihood estimator for the efficiency p_1 = (x / m) assuming a fixed ratio
    p_1 / p_2 = (x / m) / (y / n) using in the Koopman criterion, where x and y are from two
    independent binomial distributions.

    Args:
        x: number of passes in first binomial variable
        m: sample size for first binomial variable
        y: number of passes in second binomial variable
        n: sample size for second binomial variable
        th0: fixed efficiency ratio p_1 / p_2

    Returns: p_1
    """
    pp1 = prob1(x, m, y, n, th0)
    out1 = (1 / m) * (x - m * pp1) ** 2 * ((1 - pp1) + (m / n) * (th0 - pp1))
    x, m, y, n, th0 = y, n, x, m, 1 / th0
    pp2 = prob1(x, m, y, n, th0)
    out2 = (x - m * pp2) ** 2 / m * (1 - pp2 + (m / n) * (th0 - pp2))
    cond = pp1 * (1 - pp1) ** 2 < pp2 * (1 - pp2) ** 2
    num = np.where(cond, out2, out1)
    div = np.where(cond, pp2 * (1 - pp2) ** 2, pp1 * (1 - pp1) ** 2)
    out = num / div

    return out


def UU(x, m, y, n, th0):
    """different (numerically unstable)  version of U to test whether I get same results"""
    pp1 = prob1(x, m, y, n, th0)
    t = [(z - q * Z) ** 2 / (Z * q * (1 - q)) for z, Z, q in [(x, m, pp1), (y, n, pp1 / th0)]]
    return sum(t)


def UUU(x, m, y, n, th0):
    """different (numerically unstable)  version of U to test whether I get same results"""
    pp1 = prob1(x, m, y, n, th0)
    out1 = (1 / m) * (x - m * pp1) ** 2 * ((1 - pp1) + (n / m) * (th0 - pp1))
    return out1 / (pp1 * (1 - pp1) ** 2)


def koopman_confint(
    x: float | int,
    m: float | int,
    y: float | int,
    n: float | int,
    significance: float = 0.317,
):
    """
    calculate a confidence interval for the binomial ratio (x / m) / (y / n) where x and y are from two
    independent binomial distribution according to the Coopman criterion.

    Args:
        x: number of passes in first binomial variable
        m: sample size for first binomial variable
        y: number of passes in second binomial variable
        n: sample size for second binomial variable
        significance: rate of false positives

    Returns: lower limit, upper limit

    """

    # equation to solve: U = (1 - significance) percentile of the chi2 distribution with one dof
    chival = chi2.ppf(1 - significance, 1)
    func = lambda th: U(x, m, y, n, th) - chival

    # report negative values and set to zero
    values = ["x", "m", "y", "n"]
    for i, v in enumerate(values):
        if (vv := eval(v)) < 0:
            warnings.warn(
                f"found negative count in calculating confindence interval for (x / m) / (y / n): {v} = {vv}",
            )
            vv = 0
        values[i] = vv

    x, m, y, n = values

    # check whether either x or y is (practically) zero
    zx = np.abs(x) < 1e-13
    zy = np.abs(y) < 1e-13

    # if both x and y are zero, return 0, np.inf
    if zx and zy:
        return 0, np.inf

    # check whether counts are zero.
    # The ratio can then be anywhere between 0 and 1
    if np.abs(m) < 1e-13:
        return 0, 1 / (y / n)

    if np.abs(n) < 1e-13:
        return x / m, np.inf

    # if either x or y are non-zero, then one solution to the
    first_attempts = [0.5, 1., 1.5]
    for fa in first_attempts:
        try:
            first_root = opt.newton(func, fa)  # change 0.5 if needed
            break
        except:
            pass
    else:
        logger.warning(
            "couldn't find solution for confidence interval for (x / m) / (y / n) with "
            f"x = {x}, m = {m}, y = {y}, n = {n}. Return 0, inf",
        )
        return 0, np.inf

    # if x is zero, apply lower limit of zero, and upper limit the found solution
    if zx:
        return 0, first_root
    # if y is zero, apply upper limit of +infty, and lower limit the found solution
    elif zy:
        return first_root, np.inf
    else:
        # the found solution "first_root" can be either a lower limit or upper limit. In the former case,
        # the target function "func" should be decreasing at "first_root" hence "func((1 + eps) * first_root) < 0"
        # for "eps" small enough. If we find such an eps, than the upper limit should lie somewhere in the interval
        # ((1 + eps) * first_root, +infty) and one can  look for the root by taking ever larger intervals until a root
        # if found.
        # If "first_root" is a upper limit, the above reasoning still applies but in reverse. Hence, one finds
        # eps with "func((1 - eps) * first_root) < 0", and looks for the lower limit in (0, (1 + eps) * first_root)

        eps = 0.1

        # limit on smallest eps to avoid infinite loop
        while eps > 1e-19:

            # test if "first_root" is a lower limit
            if func((1 + eps) * first_root) < 0:

                # go further to the right in ((1 + eps) * first_root, +infty) to find where "func > 0" again
                up = (1 + eps) * first_root
                while func(up) < 0 or np.isnan(func(up)):
                    up *= (1 + eps)

                # find the second solution which gives the upper limit
                return first_root, opt.brentq(func, up / (1 + eps), up)

            # test if "first_root" is a upper limit
            elif func((1 - eps) * first_root) < 0:

                # go further to the left in (0, (1 + eps) * first_root) to find where "func > 0" again
                dn = (1 - eps) * first_root
                while func(dn) < 0 or np.isnan(func(dn)):
                    dn *= (1 - eps)

                # find the second solution which gives the lower limit
                return opt.brentq(func, dn, dn / (1 - eps)), first_root

            # if both of the above tests failed, "eps" is too large, so try again with smaller "eps"
            eps /= 10

    # the above calculation should as far as I know now always converge, but who knows
    raise AssertionError("error calculation could not converged")


def plottest(x, m, y, n, significance=0.317):
    """ test Koopman intervals with plot showing U as a function of theta and the found solutions """
    chival = chi2.ppf(1 - significance, 1)
    tests = np.linspace(0, 10, 10000)

    try:
        thdn, thup = koopman_confint(x, m, y, n, significance)
    except:
        print("no solution found")
        thdn, thup = (np.nan,) * 2
    plt.plot(tests, U(x, m, y, n, tests) - chival)
    plt.axhline(0)
    plt.axvline(thdn)
    plt.axvline(thup)
    plt.ylim([-30, 100])
    plt.show()


if __name__ == "__main__":
    inputs = (1, 8, 0.0, -.1774)
    koopman_confint(*inputs)
    plottest(*inputs)
