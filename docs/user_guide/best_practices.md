# Best practices


## Columnflow convenience tools

- Columnflow defines {py:attr}`~columnflow.columnar_util.EMPTY_FLOAT`, a float variable
containing the value ```-99999.0```. This variable is typically used to replace null values
in awkward arrays.

- In many cases, one wants to access an object that does not exist for every event (e.g. accessing
the transverse momentum of the 3rd jet ```events.Jet[:, 2].pt```, even though some events may only
contain two jets). In that case, the {py:class}`~columnflow.columnar_util.Route` class and its
{py:meth}`~columnflow.columnar_util.Route.apply` function can be used to access this object by
replacing missing values with the given value, e.g.
```jet3_pt = Route("Jet.pt[:, 2]").apply(events, null_value=EMPTY_FLOAT)```.

- Columnflow allows the use of some fields out of the ```events``` array, like the ```Jet``` field,
as a Lorentz vector to apply operations on. For this purpose, you might use the
{py:func}`~columnflow.production.util.attach_coffea_behavior` function. This
function can be applied on the ```events``` array using
```python
events = self[attach_coffea_behavior](events, **kwargs)
```
If the name of the field does not correspond to a standard coffea field name, e.g. "BtaggedJets",
which should provide the same behaviour as a normal jet, the behaviour can still be set, using
```python
collections = {x: {"type_name": "Jet"} for x in ["BtaggedJets"]}
events = self[attach_coffea_behavior](events, collections=collections, **kwargs)
```


- shortcuts / winning strategies / walktrough guides e.g. pilot parameter
TODO

- config utils: TODO

- categorization
   - mutually exclusive leaf categories
TODO

## General advices

- When storage space is a limiting factor, it is good practice to produce and store (if possible)
columns only after the reduction, using the {py:class}`~columnflow.tasks.production.ProduceColumns`
task.



## Using python scripts removed from the standard workflow

- Use a particular cf_sandbox for a python script not implemented in the columnflow workflow: Write
```cf_sandbox venv_columnar_dev bash ``` on the command line.

- Call tasks objects in a python script removed from the standard workflow: An imported task can
be run through the ```law_run()``` command, its output can be accessed through the "output"
function of the task. An example is given in the following code snippet.

```python
from columnflow.tasks.selection import SelectEvents

# run some task
task = SelectEvents(version="v1", dataset="tt_sl_powheg", walltime="1h")
task.law_run()

# do something with the output
output = task.output()["collection"]
```






