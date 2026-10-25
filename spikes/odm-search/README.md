# odm-search

Question: how do Atlas Search clauses and a Beanie document layer behave on the engine container and on the hosted cluster, in the places where the engine fails silently or the document layer has its own rules?

One harness (`run.py`), eight checks, each prints rows and writes `results/<label>.json`. An engine error inside a case is an observation; a harness error fails the run.

| check | what it observes |
|---|---|
| c1 | the relevance score added next to each parsed document (`$meta: searchScore`), against a plain find; score under a filter-only query; a cap applied to the total |
| c2 | scoring clauses in `must`, `filter`, `should`, `mustNot` (alone and combined), `minimumShouldMatch`, an empty compound |
| c3 | what each index definition lets a clause do (`equals`, `in`, `range`, `regex`, `text`, `phrase`, `autocomplete`, unmapped paths): hits, empty or error, against a written prediction table |
| c4 | an index named `default`, queried with and without an index name; a missing index name |
| c5 | `highlight` through a projection model; `autocomplete`, fuzzy, unmapped path |
| c6 | saving a document whose link field holds a fetched or constructed document (`save`, `save(link_rule=WRITE)`, `save_changes`) |
| c7 | an explicit session that is not in a transaction: a write with and without a check before it |
| c8 | scale: time to searchable, page latency by depth, page consistency (duplicates, order), query against parse time |

Arms: engine container and hosted cluster are separate jobs; compare within one run only. Hosted limits (index count, operations per second, indexing delay) belong to that tier.

```
uv sync
python run.py engine --label local --docs 1500 --reps 2     # needs the container on ODM_URL (default port 27300)
python run.py hosted --label h --docs 20000 --reps 5         # needs HOODIE_BENCH_ATLAS_URL; Actions only
```
