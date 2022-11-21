# coding: utf-8

"""
A dummy script to test config objects
"""

from plhld.config.config_2017 import config_2017 as cfg

print(" ================ Processes ======================")
process_insts = cfg.processes
for proc_inst in process_insts:
    print(proc_inst.name)

print("================= Datasets =======================")
dataset_insts = cfg.datasets
for data_inst in dataset_insts:
    print(data_inst.name)

print("================= Categories =====================")
category_insts = cfg.categories
for cat_inst in category_insts:
    print(cat_inst.name)

print("================= Variables ======================")
variable_insts = cfg.variables
for var_inst in variable_insts:
    print(var_inst.name)

print("================= Shifts =========================")
shift_insts = cfg.shifts
for shift_inst in shift_insts:
    print(shift_inst.name)

print("================= Auxiliary ======================")
aux = cfg.aux
for key, value in aux.items():
    print(key)

# print some features of an exemplary process inst
proc_inst = cfg.get_process("tt")
print(f"================= Process: {proc_inst.name} ======================")
print("Label:", proc_inst.label)
print("Color:", proc_inst.color)
print("Cross section:", proc_inst.get_xsec(13).str())
if not proc_inst.is_leaf_process:
    print("Leaf processes:", [proc.name for proc in proc_inst.get_leaf_processes()])

# get all datasets corresponding to the tt process or a sub process
sub_process_insts = [p for p, _, _ in proc_inst.walk_processes(include_self=True)]
dataset_insts = [
    dataset_inst for dataset_inst in cfg.datasets
    if any(map(dataset_inst.has_process, sub_process_insts))
]
print(f"{proc_inst.name} datasets:", [d.name for d in dataset_insts])

# print some features of an exemplary dataset inst
dataset_inst = cfg.get_dataset("tt_sl_powheg")
print(f"================= Dataset: {dataset_inst.name} ============")
print("N events:", dataset_inst.n_events)
print("N files:", dataset_inst.n_files)
print("Processes:", [p.name for p in dataset_inst.processes])
print("Aux:", dataset_inst.aux)

# print some features of an exemplary category inst
cat_inst = cfg.get_category("1mu")
print(f"================= Category: {cat_inst.name} ====================")
print("Label:", cat_inst.label)
print("Selection:", cat_inst.selection)
if not cat_inst.is_leaf_category:
    print("Leaf categories:", [leaf_cat.name for leaf_cat in cat_inst.get_leaf_categories()])

# print some features of an exemplary variable inst
var_inst = cfg.get_variable("ht")
print(f"================= Variable: {var_inst.name} =====================")
print("Expression:", var_inst.expression)
print("Binning", var_inst.binning)
print("x title", var_inst.x_title)
print("y title", var_inst.y_title)
print("log_y", var_inst.log_y)
print("unit", var_inst.unit)

# print some features of an exemplary shift inst
shift_inst = cfg.get_shift("minbias_xs_up")
print(f"================= Shift: {shift_inst.name} =============")
print("Label:", shift_inst.label)
print("Type:", shift_inst.type)
print("Direction:", shift_inst.direction)
print("Aliases:", shift_inst.x.column_aliases)

# get some exemplary aux (all 3 methods get you the same result)
default_selector = cfg.get_aux("default_selector")
default_selector = cfg.aux["default_selector"]
default_selector = cfg.x.default_selector
print("================= default selector:", default_selector, "=======")

# set some exemplary aux youself
cfg.set_aux("example", "test")
print(cfg.x.example)
cfg.x.example = "another test"
print(cfg.x.example)

# opens a debugger for trying out yourself
__import__("IPython").embed()
