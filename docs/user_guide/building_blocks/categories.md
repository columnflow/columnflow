```{mermaid}
:name: plotting-tasks
:theme: default
:zoom:
:align: center
:caption: CategoriesMixin
classDiagram
    CategoriesMixin <|-- CreateYieldsTable
    CategoriesMixin <|-- PlotVariablesBase
    CategoriesMixin <|-- PlotCutflowBase

    class CategoriesMixin{
        categories: law.CSVParameter
        default_categories: None
        allow_empty_categories: False
    }
```


# Categories

In Columnflow, there are many tools to create a complex and flexible categorization of all analysed
events. This guide presents, how to implement a set of categories in Columnflow and presents an
showcases, how to use the resulting categories via the
{py:class}`~columnflow.tasks.yields.CreateYieldsTable` task.



## How to use categories?

Assuming you used the analysis template to setup your analysis, there are already two categories
included, named ```incl``` and ```2j``` included. To test, that the categories are properly implemented,
we can use the CreateYieldsTable task:
```shell
law run cf.CreateYieldsTable --version v1 \
    --calibrators example --selector example --producers category_ids \
    --processes tt,st --categories incl,2j
```

When all tasks are finished, this should create a table presenting the event yield of each process
for all categories. To create new categories, we essentially need three ingredients:

1. We need to define our categories in the config
```{literalinclude} ../../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:language: python
:start-at: add_category(
:end-at: )
```
2. We need to write a {py:class}`~columnflow.categorization.Categorizer` that defines, which events
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
TODO
:::


:::{dropdown} And where are the ```category_ids``` actually used?
TODO
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

# TODO: make code

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
    # our `category_ids` producer can automatically combine multiple selections via logical 'and'
    selection=[cat_1mu.selection, cat_2jet.selection],
    label="1 muon, 0 electrons, 2 jets",
)
cat_1e__3jet = cat_3jet.add_category(
    name="1e__3jet",
    id=cat_3jet.id + cat_1e.id,
    # our `category_ids` producer can automatically combine multiple selections via logical 'and'
    selection=[cat_1e.selection, cat_3jet.selection],
    label="1 electron, 0 muons, 3 jets",
)
cat_1mu__3jet = cat_3jet.add_category(
    name="1e__3jet",
    id=cat_3jet.id + cat_1mu.id,
    # our `category_ids` producer can automatically combine multiple selections via logical 'and'
    selection=[cat_1mu.selection, cat_3jet.selection],
    label="1 muon, 0 electrons, 3 jets",
)

# add childs also to lepton categories
cat_1e.add_category(cat_1e__2jet)
cat_1e.add_category(cat_1e__3jet)
cat_1mu.add_category(cat_1mu__2jet)
cat_1mu.add_category(cat_1mu__3jet)
```

:::{dropdown} Note! We only store leaf category ids!
TODO
:::

Let's test if our leaf categories are working as intended:

```shell
law run cf.CreateYieldsTable --version v1 \
    --calibrators example --selector example --producer example \
    --processes tt,st --categories 1e,2jet,1e__2jet,1e__3jet,1mu__2jet,1mu__3jet
```

We should see that the yield of the "2jet" category is the same as the "1e__2jet" and "1mu__2jet"
categories combined. This is to be expected, since the "2jet" category will be built by combining
histograms from all of their leaf categories.

:::{dropdown} Note! Take care to define your categories orthogonal!
Double counting otherwise
TODO
:::

## Categorization with multiple layers

But what should you do when the number of category combinations is getting large? For example, you might
want to define categories based on the number of leptons, the number of jets, and based on a simple
obervable such as ```HT```:

```python
jet_categories = (2, 3, 4, 5, 6)
lep_categories = (1, 2, 3)
ht_categories =  ((0, 200), (200, 400), (400, 600))
```

If you want to build all combinations of this set of 5+3+3 categories, you end up with $5*3*3=45$
leaf categories. To build all of them by hand is very tedious, unflexible, and error prone.
Luckily, Columnflow provides all necessary tools to build combinations of categories automatically.
To use these tools, we first need to create our base categories and their corresponding Categorizers.

```python

#
# add categories to config
#

for n_lep in lep_categories:
    add_category(
        config,
        id=10 * n_lep,
        name=f"{n_lep}lep",
        selection=f"cat_{n_lep}lep",
        label=f"{n_lep} leptons",
    )
for n_jet in jet_categories:
    add_category(
        config,
        id=100 * n_jet,
        name=f"{n_jet}jet",
        selection=f"cat_{n_jet}jet",
        label=f"{n_jet} jets",
    )
for i, (ht_down, ht_up) in enumerate(ht_categories):
    add_category(
        config,
        id=1000 * i,
        name=f"ht_{ht_down}to{ht_up}",
        selection=f"cat_ht{ht_down}to{ht_up}",
        label=f"HT in ({ht_down}, {ht_up}]",
    )

#
# define Categorizer modules
#

for n_jet in jet_categories:
    @categorizer(name="cat_{n_jet}jet", uses={"Jet.pt"})
    def cat_n_jet(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
        return events, ak.num(events.Jet.pt, axis=1) == n_jet

for n_lep in (1, 2, 3):
    @categorizer(name="cat_{n_lep}lep", uses={"Electron.pt", "Muon.pt"})
    def cat_n_lep(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
        return events, (ak.num(events.Electron.pt, axis=1) + ak.num(events.Muon.pt, axis=1)) == n_lep

for ht_down, ht_up in ht_categories:
    @categorizer(name="cat_ht{ht_down}to{ht_up}", uses={"Jet.pt"})
    def cat_ht(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
        ht = ak.sum(events.Jet.pt, axis=1)
        return events, ((ht > ht_down) & (ht <= ht_up))
```


(TODO: text)

How to combine lots of categories

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

# this function creates all possible category combinations without producing duplicates
# (e.g. category '1lep__2jet__ht200to400' will be produced, but '2jet__1lep__ht200to400' will be skipped)
n_cats = create_category_combinations(
    config,
    category_blocks,
    name_fn=name_fn,
    kwargs_fn=kwargs_fn,
    skip_existing=False,
)

```
