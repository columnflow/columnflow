Work in Progess! This guide has not been finalized and the presented code has not yet been tested.

TODO: make the code testable

# Categories

In Columnflow, there are many tools to create a complex and flexible categorization of all analysed
In columnflow, there are many tools to create a complex and flexible categorization of all analysed
events.
Generally, this categorization can be layered.
We refer to the smallest building block of these layers as **leaf categories**, which can subsequently be either run individually or combined into more complex categories.
This guide presents how to implement a set of categories in columnflow and presents an
shows how to use the resulting categories via the {py:class}`~columnflow.tasks.yields.CreateYieldTable` task.



## How to use categories?

If you used the analysis template to setup your analysis, there are already two categories
included, named ```incl``` and ```2j```.
To test that the categories are properly implemented,
we can use the CreateYieldsTable task:
```shell
law run cf.CreateYieldTable --version v1 \
    --calibrators example --selector example --producers category_ids \
    --processes tt,st --categories incl,2j
```

When all tasks are finished, this should create a table presenting the event yield of each process
for all categories. To create new categories, we essentially need three ingredients:

1. We need to define our categories as {external+order:py:class}`order.category.Category` instances in the config
```{literalinclude} ../../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:language: python
:start-at: add_category(
:end-at: )
```
2. We need to write a {py:class}`~columnflow.categorization.Categorizer` that defines which events
to select for each category. The name of the Categorizer needs to be the same as the "selection"
attribute of the category inst.
```{literalinclude} ../../../analysis_templates/cms_minimal/__cf_module_name__/categorization/example.py
:language: python
```
Keep in mind that the module in which your define your Categorizer needs to be included in the law config

3. We need to run the {py:class}`~columnflow.production.categories.category_ids` Producer to create
the ```category_ids``` column. This is directly included via the ```--producers category_ids```,
but you could also run the ```category_ids``` Producer as part of another Selector or Producer.
```python
events = self[category_ids](events, **kwargs)
```

:::{dropdown} But how does this actually work?
The `category_ids` Producer loads all category instances from your config. For each leaf category inst
(which is the "smallest unit" of categories, more to that later),
it maps the `category_inst.selection` string to a `Categorizer` and adds it the to the `uses` and
`produces`, meaning that columns required (produced) by the `Categorizer` will automatically be
loaded (stored) when running the `category_ids` Producer.

During the event processing, the `Categorizer` of each leaf category is evaluated to generate
a mask, which defines, whether the event is part of this category or not.
The mask is then transformed to an array of ids (either the `category_inst.id` if True, and
`None` for False entries).

In the end, we return a jagged array of category ids, which allows us to categorize one event
into multiple different types of categories.

You can also store a list of strings in the `category_inst.selection` field. In that case, the logical
`and` of all masks obtained by the `Categorizer` is used to define the mask corresponding to this
category.
:::


:::{dropdown} And where are the ```category_ids``` actually used?
The `category_ids` column is primarily used when creating our histograms (e.g. in the
{py:class}`~columnflow.tasks.histograms.CreateHistograms` task).
The created histograms always contain one axis for categories, using the values from the `category_ids`
column. Since this column is a jagged array, it is possible to fill events either never or multiple
times in a histogram.

Other tasks such as {py:class}`~columnflow.tasks.plotting.PlotVariables1D` are then using this
axis with categories to obtain all entries from the histogram corresponding to the category
that is requested via the `--categories` parameter. When the given category is not a leaf category
but contains categories itself, the ids of all its leaf categories combined are used for the category.
:::


## Creation of nested categories

Often times, there are multiple layers of categorization. For example, we might want to categorize
based on the number of leptons and the number of jets.

```python
# do 0 lepton vs >= 1 lepton instead?
cat_1e = config.add_category(
    name="1e",
    id=10,
    selection="cat_1e",
    label="1 Electron, 0 Muons",
)
cat_1mu = config.add_category(
    name="1mu",
    id=20,
    selection="cat_1mu",
    label="1 Muon, 0 Electrons",
)
cat_2jet = config.add_category(
    name="2jet",
    id=200,
    selection="cat_2jet",
    label=r"$N_{jets} \leq 2$",
)
cat_3jet = config.add_category(
    name="3jet",
    id=300,
    selection="cat_3jet",
    label=r"$N_{jets} \geq 3$",
)
```

```python
@categorizer(uses={"Jet.pt"})
def cat_2jet(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    return events, ak.num(events.Jet.pt, axis=1) <= 2

@categorizer(uses={"Jet.pt"})
def cat_3jet(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    return events, ak.num(events.Jet.pt, axis=1) >= 3

@categorizer(uses={"Muon.pt", "Electron.pt"})
def cat_1e(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    return events, (ak.num(events.Muon.pt, axis=1) == 0 & ak.num(events.Electron.pt, axis=1) == 1)

@categorizer(uses={"Muon.pt", "Electron.pt"})
def cat_1mu(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    return events, (ak.num(events.Muon.pt, axis=1) == 1 & ak.num(events.Electron.pt, axis=1) == 0)
```

This code snippet produces 4 new categories, which (after rerunning the ```category_ids```
producer and recreating all necessary histograms) can already be used:

```shell
law run cf.CreateYieldsTable --version v1 \
    --calibrators example --selector example --producer category_ids \
    --processes tt,st --categories "*"
```


As a next step, one might want to combine these categories to categorize based on the number of
jets and leptons simutaneously. You could simply create all combinations by hand. The following code
snippet shows how to combine the "2jet" and the "1lep" category:

```python
cat_2jet = config.get_category("2jet")
cat_1lep = config.get_category("1lep")

# add combined category as child to the existing categories
cat_1e__2jet = cat_2jet.add_category(
    name="1e__2jet",
    id=cat_2jet.id + cat_1e.id,
    # our `category_ids` producer can automatically combine multiple selections via logical 'and'
    selection=[cat_1e.selection, cat_2jet.selection],
    label="1 electron, 0 muons, >2 jets",
)
cat_1mu__2jet = cat_2jet.add_category(
    name="1e__2jet",
    id=cat_2jet.id + cat_1mu.id,
    selection=[cat_1mu.selection, cat_2jet.selection],
    label="1 muon, 0 electrons, 2 jets",
)
cat_1e__3jet = cat_3jet.add_category(
    name="1e__3jet",
    id=cat_3jet.id + cat_1e.id,
    selection=[cat_1e.selection, cat_3jet.selection],
    label="1 electron, 0 muons, 3 jets",
)
cat_1mu__3jet = cat_3jet.add_category(
    name="1e__3jet",
    id=cat_3jet.id + cat_1mu.id,
    selection=[cat_1mu.selection, cat_3jet.selection],
    label="1 muon, 0 electrons, 3 jets",
)

# add children also to lepton categories
cat_1e.add_category(cat_1e__2jet)
cat_1e.add_category(cat_1e__3jet)
cat_1mu.add_category(cat_1mu__2jet)
cat_1mu.add_category(cat_1mu__3jet)
```

We now created categories with dependencies between each other. In columnflow, we always use the
smallest units of categories (commonly called "leaf categories") to build our parent categories. For
example, the `2jet` category consists of the leaf categories `1e__2jet` and `1mu__2jet`. To select
events corresponding to `2jet`, we will add all events with category ids corresponding to either
`1e__2jet` or `1mu__2jet`.


:::{note} We might add unexpected selections by combining categorization.

In this example, the `cat_1e` and `cat_1mu` Categorizer only select events with exactly one lepton
(either electron or muon). This means, that events with zero or multiple leptons will not be considered
in our categorization, even when building e.g. the `2jet` category.
:::

Let's test if our leaf categories are working as intended:

```shell
law run cf.CreateYieldTable --version v1 \
    --calibrators example --selector example --producer category_ids \
    --processes tt,st --categories 1e,2jet,1e__2jet,1e__3jet,1mu__2jet,1mu__3jet
```

We should see that the yield of the `2jet` category is the same as the `1e__2jet` and `1mu__2jet`
categories combined. This is to be expected, since the `2jet` category will be built by combining
histograms from all of their leaf categories.

:::{dropdown} Note! Take care to define your categories orthogonal!
There is no mechanism that automatically prevents double counting. It is therefore essential to define
categories such that there is no overlap between leaf categories of any defined category.

Example:
We might have two categories selecting either exactly two or at least two jets.

```python
cat_2jet = config.add_category(
    name="2jet",
    id=200,
    selection="cat_2jet",
    label=r"$N_{jets} = 2$",
)
cat_geq2jet = config.add_category(
    name="geq2jet",
    id=300,
    selection="cat_geq2jet",
    label=r"$N_{jets} \geq 2$",
)

@categorizer(uses={"Jet.pt"})
def cat_2jet(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    return events, ak.num(events.Jet.pt, axis=1) == 2

@categorizer(uses={"Jet.pt"})
def cat_geq2jet(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    return events, ak.num(events.Jet.pt, axis=1) >= 2
```

This can be done without any issues since these two categories are independent of each other
(they will each produce their own category id).
However, if we now add a parent category that contains both `2jet` and `geq2jet`, this will result
in couting all events twice that contain exactly two jets.

```python
non_orthogonal_cat = config.add_category(
    name="non_orthogonal_cat",
    id=100,
    label="Exactly two and at least two jets, resulting in double-counting. You should not do this",
)
non_orthogonal_cat.add_category(cat_2jet)
non_orthogonal_cat.add_category(cat_geq2jet)
```
:::

## Categorization with multiple layers

But what should you do when the number of category combinations is getting large? For example, you might
want to define categories based on the number of leptons, the number of jets, and based on a simple
obervable such as ```HT```:

```python
jet_categories = (2, 3, 4, 5, 6)
lep_categories = (1, 2, 3)
ht_categories = ((0, 200), (200, 400), (400, 600))
```

If you want to build all combinations of this set of 5+3+3 categories, you end up with 5 x 3 x 3 = 45
leaf categories. To build all of them by hand is very tedious, unflexible, and error prone.
Luckily, columnflow provides all necessary tools to build combinations of categories automatically.
To use these tools, we first need to create our base categories and their corresponding Categorizers.

```python
#
# add categories to config
#

for n_lep in lep_categories:
    config.add_category(
        id=10 * n_lep,
        name=f"{n_lep}lep",
        selection=f"cat_{n_lep}lep",
        label=f"{n_lep} leptons",
    )
for n_jet in jet_categories:
    config.add_category(
        id=100 * n_jet,
        name=f"{n_jet}jet",
        selection=f"cat_{n_jet}jet",
        label=f"{n_jet} jets",
    )
for i, (ht_down, ht_up) in enumerate(ht_categories):
    config.add_category(
        id=1000 * i,
        name=f"ht_{ht_down}to{ht_up}",
        selection=f"cat_ht{ht_down}to{ht_up}",
        label=f"HT in ({ht_down}, {ht_up}]",
    )

#
# define Categorizer modules
#

for n_jet in jet_categories:
    @categorizer(name=f"cat_{n_jet}jet", uses={"Jet.pt"})
    def cat_n_jet(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
        return events, ak.num(events.Jet.pt, axis=1) == n_jet

for n_lep in (1, 2, 3):
    @categorizer(name=f"cat_{n_lep}lep", uses={"Electron.pt", "Muon.pt"})
    def cat_n_lep(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
        return events, (ak.num(events.Electron.pt, axis=1) + ak.num(events.Muon.pt, axis=1)) == n_lep

for ht_down, ht_up in ht_categories:
    @categorizer(name=f"cat_ht{ht_down}to{ht_up}", uses={"Jet.pt"})
    def cat_ht(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
        ht = ak.sum(events.Jet.pt, axis=1)
        return events, ((ht > ht_down) & (ht <= ht_up))
```

This will only create the (5+3+3) parent categories. To create all possible combinations of
these categories, we can use the {py:func}`~columnflow.config_util.create_category_combinations`
function that will do most of the work for us:

```python
def name_fn(root_cats):
    """ define how to combine names for category combinations """
    cat_name = "__".join(cat.name for cat in root_cats.values())
    return cat_name


def kwargs_fn(root_cats):
    """ define how to combine category attributes for category combinations """
    kwargs = {
        "id": sum([c.id for c in root_cats.values()]),
        "label": ", ".join([c.name for c in root_cats.values()]),
        # optional: store the information on how the combined category has been created
        "aux": {
            "root_cats": {key: value.name for key, value in root_cats.items()},
        },
    }
    return kwargs

#
# define how to combine categories
#


category_blocks = OrderedDict({
    "lep": [config.get_category(f"{n_jet}lep") for n_lep in lep_categories],
    "jet": [config.get_category(f"{n_jet}jet") for n_jet in jet_categories],
    "ht": [config.get_category(f"ht{ht_down}to{ht_up}") for ht_down, ht_up in ht_categories],
})

from columnflow.config_util import create_category_combinations

# this function creates all possible category combinations without producing duplicates
# (e.g. category '1lep__2jet__ht200to400' will be produced, but '2jet__1lep__ht200to400' will be skipped)
n_cats = create_category_combinations(
    config,
    category_blocks,
    name_fn=name_fn,
    kwargs_fn=kwargs_fn,
    skip_existing=False,
)

# let's check what categories have been produced
print(f"{n_cats} have been created by the create_category_combinations function")
all_cats = [cat.name for cat, _, _ in config.walk_categories()]
print(f"List of all cateogries in our config: \n{all_cats}")
```

To test our final set of categories, we can call our `CreateYieldTable` task again.

```shell
law run cf.CreateYieldTable --version v1 \
    --calibrators example --selector example --producer category_ids \
    --processes tt,st --categories "*"
```


:::{note} This categorization is not inclusive!

Since we only store leaf category ids, all the events that are not considered by one of the category
blocks will not be considered at all. For this set of categories it means that we implicitly added cuts
on the number of jets (>= 2 and <= 6), the number of leptons (>= 1 and <= 3), and HT (<= 600) as soon
as we use one of these categories.

TODO: it might be more instructive to build this example such that our categorization is inclusive.
:::


## Extend your categorization as part of a Producer

Some of your categories might need some computationally expensive reconstructed observables
and can therefore only be called after certain requirements are met.
In these cases, it might be beneficial to first produce columns and then load these columns when necessary.
To streamline this, you can set these dependencies as part of the Producer instance that creates the category ids (i.e. via the `category_ids` producer).

To properly set this up, add the category instances as part of the `Producer.init`. Columns from
custom requirements can be added to a Producer via the `Producer.requires` in combination
with the `Producer.setup`.

```python
@producer(
    uses={category_ids},
    produces={category_ids},
    ml_model_name="my_ml_model",
)
def my_categorization_producer(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    # do whatever else this Producer needs to to
    ...

    # reproduce category ids
    events = self[category_ids](events, **kwargs)

    return events


# The *requires* function can be used to add custom requirements to your Producer task call.
# In this example, we add the MLEvaluation output to our requirements, e.g. to
# categorize events based on a DNN output score
@my_categorization_producer.requires
def my_categorization_producer_reqs(self: Producer, reqs: dict) -> None:
    if "ml" in reqs:
        return

    from columnflow.tasks.ml import MLEvaluation
    reqs["ml"] = MLEvaluation.req(self.task, ml_model=self.ml_model_name)


# To also load columns from our custom requirements, we need to tell our Producer, how to access
# columns from the inputs. This is achieved via this *setup* function
@my_categorization_producer.setup
def my_categorization_producer_setup(self: Producer, reqs: dict, inputs: dict, reader_targets: InsertableDict) -> None:
    reader_targets["mlcolumns"] = inputs["ml"]["mlcolumns"]


# This *init* function is used to add categories as part of the Producer init. This means that
# your config will always contain this category as long as you're running some task where this
# particular Producer has been requested.
@my_categorization_producer.init
def my_categorization_producer_init(self: Producer) -> None:
    # ensure that categories are only added to the config once
    tag = f"{my_categorization_producer_init.__name__}_called"
    if self.config_inst.has_tag(tag):
        return

    # add categories to config inst
    self.config_inst.add_category(
        name="my_category",
        id=12345,
        selection="my_categorizer",
    )
```


## Helper functions for categories

### get_events_from_categories

To obtain the events of a certain category (or categories) of an akward array `events` that contains the
`category_ids` column, you can use the  {py:func}`~columnflow.util.get_events_from_categories` function.


```python
from columnflow.util import get_events_from_cateogries

# you can pass a string of a category in combination with the config inst
selected_events = get_events_from_cateogries(events, "my_category", config_inst)

# it is also possible to directly pass a category. In that case, no config inst is needed
selected_events = get_events_from_cateogries(events, config_inst.get_category("my_category"))

# it is also possible to pass a list of categories. In that case, all events
# that belong to one of the requested categories are selected
selected_events = get_events_from_cateogries(events, ["cat1", "cat2"], config_inst)
```

This function automatically consideres category ids of all leaf categories from the requested
categories. This is especially helpful when your category contains multiple leaf categories.


## Key points (TL;DR)

- Categories are {external+order:py:class}`order.category.Category` instances defined in the config.
- We can add categories to each other; the "smallest unit" of category is referred to as "leaf category".
- We build each category by combining all of it's leaf categories (both in columns and in histograms).
- Leaf categories need to define one or multiple {py:class}`~columnflow.categorization.Categorizer`,
which is a function that defines whether or not an event belongs in this category.
- The `category_ids` column is produced via the
{py:class}`~columnflow.production.categories.category_ids` Producer.
- Make sure that for each category, all of its leaf categories are defined orthogonal to prevent double counting.
- Groups of categories can be combined via {py:func}`~columnflow.config_util.create_category_combinations`.
- Make sure that each group of categories is inclusive; otherwise, all combined categories will not be inclusive aswell.
