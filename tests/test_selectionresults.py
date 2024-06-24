import awkward as ak
import numpy as np

import law
from columnflow.selection import SelectionResult

logger = law.logger.get_logger(__name__)


def col_neutral_print(print_func):
    def new_print(*args, **kwargs):
        kwargs["end"] = kwargs.get("end", "\n") + "\033[0m"
        print_func(*args, **kwargs)
    return new_print


print = col_neutral_print(print)

print("\n\033[95mcorrect object mask tests:")
masks = [
    ak.Array([[2, 3], [1]]),
    ak.Array([[2], [1, None]])[:, :1],
    ak.Array([[1], [True, False]]),
    ak.Array([[1, 0, None], [True, False]])[:, :2],
    np.array([[True, False]]),
    np.array([[2, 3]]),
    ak.Array([[2, 3], [1, None]])[:, 0],
    ak.Array([[1, 0], [1, 1]])
]
s = SelectionResult(objects={"Muon": {f"m{i}": m for i, m in enumerate(masks)}})

for i, m in enumerate(masks):
    new_m = s.objects["Muon"][f"m{i}"]
    print(m, "\033[92m", new_m)
    print("\033[94m", ak.Array(m).type, "\033[92m", new_m.type)


print("\n\033[95mwrong object mask tests:")
wrong = [
    ak.Array([[2, 3], [1, True]]),
    ak.Array([[1, 2], None]),
    ak.Array([[True, False], [None]]),
    ak.Array([[True, False], None]),
    ak.Array([[True, False], None])[:1],
    ak.Array([True, False]),
    ak.Array([1, 2, True]),
]

for i, wr in enumerate(wrong):
    try:
        SelectionResult(objects={"Muon": {f"Muon{i}": wr}})
        print(wr, "\033[92m", "no error?")
    except AssertionError as e:
        print(wr, "\033[91m", e)

print("\n\033[95mpotentially problematic object mask tests:")
masks = [
    (
        ak.Array([[1, 0], [1, 1, True]])[:, :2],
       "mask is of union type but in fact only contains integer 0's and 1's. " \
       "This will lead to conversion to True and False. But seems unlikely to happen."
    )
]
s = SelectionResult(objects={"Muon": {f"m{i}": m for i, (m, _) in enumerate(masks)}})

for i, (m, discussion) in enumerate(masks):
    new_m = s.objects["Muon"][f"m{i}"]
    print(m, "\033[92m", new_m)
    print("\033[94m", ak.Array(m).type, "\033[92m", new_m.type)
    print(discussion)

print("\n\033[95mcorrect event mask tests:")
masks = [
    ak.Array([1, 0, 0, None])[:-1],
    ak.Array([1, 0, 0, 1]),
    ak.Array([0, True, 0, False]),
    ak.Array([True, True, None])[:-1],
    ak.Array([True, True, False]),
    np.array([True, True, False]),
]
for m in masks:
    new_m = SelectionResult(event=m).event
    print(m, "\033[92m", new_m)
    print("\033[94m", ak.Array(m).type, "\033[92m", new_m.type)

print("\n\033[95mwrong event mask tests:")
wrong = [
    ak.Array([True, True, None]),
    ak.Array([[True, False], [True]]),
    ak.Array([[True, False]]),
    ak.Array([[True], [False]]),
    np.array([[True], [False]]),
    ak.Array([0, 2, True]),
    ak.Array([False, 2, True]),
]

for i, wr in enumerate(wrong):
    try:
        SelectionResult(event=wr)
        print(wr, "\033[92m", "no error?")
    except AssertionError as e:
        print(wr, "\033[91m", e)

print("\n\033[95mcorrect step mask tests:")
s = SelectionResult(steps={f"s{i}": m for i, m in enumerate(masks)})

for i, m in enumerate(masks):
    new_m = s.steps[f"s{i}"]
    print(m, "\033[92m", new_m)
    print("\033[94m", ak.Array(m).type, "\033[92m", new_m.type)

print("\n\033[95mwrong step mask tests:")
for i, wr in enumerate(wrong):
    try:
        SelectionResult(steps={"s": wr})
        print(wr, "\033[92m", "no error?")
    except AssertionError as e:
        print(wr, "\033[91m", e)
