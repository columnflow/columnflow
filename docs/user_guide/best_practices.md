# Best practices


## Columnflow convenience tools

- Columnflow defines {py:data}`~columnflow.columnar_util.EMPTY_FLOAT`, an integer variable
containing the value ```-9999```. This variable can be generally used as null value without having
to considerate the difference in behaviour given by awkward array and matplotlib between integer
and ```None``` entries in an array.

- If one wants to index the ```events``` array in a way that the index does not exist for every
event (e.g. ```events.Jet.pt[10]``` does only work for events with 11 or more jets), it is possible
to use the {py:class}`~columnflow.columnar_util.Route` class and its
{py:meth}`~columnflow.columnar_util.Route.apply` function, which allows to give a default value to
the returned array in the case of a missing element without throwing an error.

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

- The weights for plotting should ideally be created in the
{py:class}`~columnflow.tasks.production.ProduceColumns` task when possible, after the selection
and reduction of the data.



## Using python scripts removed from the standard worflow

- Use a particular cf_sandbox for a python script not implemented in the columnflow workflow: Write
```cf_sandbox venv_columnar_dev bash ``` on the command line.

- Call tasks objects in a python script removed from the standard workflow:

TODO

(Require the output of a task in a general python script which is not a task again? ->
ask marcel where the example was with ".law_run()")







