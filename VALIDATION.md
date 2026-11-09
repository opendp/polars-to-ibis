# Expression-dispatch validation

## Verdict

**Not ready: numeric semantic defects remain.** The dispatch architecture itself is sound; no merger of naming and value translation or major redesign is warranted. Local fixes make the available test matrix green, but strict expected failures still demonstrate incorrect Float32 statistics. Existing quantile tests also tolerate a genuinely different operation.

Validated branch: `p2i-single-value-dispatch`, original HEAD `29649442c213c5e9b24913f0910ad9d4dfc0610c`. Comparison `origin/main`: `707a102`. The checkout was initially clean. The audit ended with local validation changes and no remote publication. Those changes are kept on the follow-up branch `p2i-single-value-dispatch-validation`; the original branch is unchanged.

## A. Test results

Environment: macOS, Python **3.11.15**, Ibis **12.0.0**, DuckDB **1.5.6**, SQLite **3.53.2**. Both database backends and all three existing exporters (`to_polars`, `to_pandas`, `to_pyarrow`) were exercised.

Commands below use `uv run --group polars-VERSION` unless noted. Both documented `uv sync --group polars-VERSION` setups succeeded.

| State | Polars | Command | Passed | Failed | Skipped | Xfailed | Deselected |
|---|---|---|---:|---:|---:|---:|---:|
| Original branch | 1.36.1 | `pytest -x -q -m 'not extra_install'` | 97 | 1 | 0 | 4 | 365 |
| Original branch | 1.37.0 | same | 76 | 1 | 2 | 0 | 355 |
| Original branch | 1.36.1 | `pytest -q -m 'not extra_install'` | 707 | 24 | 0 | 85 | 365 |
| Original branch | 1.37.0 | same | 693 | 30 | 2 | 73 | 355 |
| Main | 1.36.1 | same, main source/tests with shared environment | 716 | 0 | 0 | 91 | 353 |
| Main | 1.37.0 | same | 702 | 0 | 2 | 85 | 343 |
| Local fixes/tests | 1.36.1 | `pytest -x -q -m 'not extra_install'` | 840 | 0 | 0 | 127 | 438 |
| Local fixes/tests | 1.37.0 | same | 826 | 0 | 2 | 121 | 428 |
| Local fixes/tests | 1.36.1 | `scripts/ci.sh -m 'not extra_install'` | 840 | 0 | 0 | 127 | 438 |
| Local fixes/tests | 1.37.0 | same | 826 | 0 | 2 | 121 | 428 |

- Original filtered CI scripts also failed with the same 24/30 failures. Final 1.36.1 CI achieves **100% statement/branch coverage**; 1.37.0 CI does not enforce coverage by design.
- `uv run --group polars-1.36.1 pre-commit run --all-files`: final pass. Initially it fixed one trailing blank line; a subsequent formatting pass formatted local changes.
- PostgreSQL connection failed: no server socket at `/tmp/.s.PGSQL.5432`. Full unfiltered CI was therefore **not** run; PostgreSQL is unverified, not passing. Python 3.12/3.13 were not tested. The Ibis Polars backend is excluded from this repository's test matrix and was not validated.
- Main comparison used a detached worktree at `/tmp/p2i-validation/main`, with explicit `PYTHONPATH=src` and the shared virtualenv on `PATH`. An initial main run had an environment-only pyright/pyenv failure; rerunning with the correct PATH passed.
- The 12 new Float32 expected failures are strict assertion failures, not relaxed dtype checks. Other added expected failures explicitly document unsupported conversions. Green totals do **not** mean full Polars parity.

Original failure breakdown: six exporter/backend failures each for issue #148's stale integer expected values and literal-only selection, plus twelve stale whole-table var/std tolerance failures. Polars 1.37.0 adds six zero-column SQL failures (`FROM lf`).

## B. Confirmed behavior changes

| Change | Classification and evidence |
|---|---|
| Mixed aggregate/row-wise Select | **Bug fix (#148).** Original issue reproduction now broadcasts SUM(floats)=1.0 alongside ints on SQLite/DuckDB, all exporters. Main cannot bind the selected float column. Correct the fixture to floats, not the output to integers. PostgreSQL remains unverified. |
| Aggregate-only/nested Select | **Correct.** Arithmetic and conditionals containing aggregates produce one row; mixed expressions broadcast through Ibis windows. Empty mixed projections produce zero rows. Literal plus aggregate also works where main could not bind the reduction. |
| HStack siblings | **Bug fix.** For x=[1,2,3], replacing x with x+1 and adding y=x+2 gives x=[2,3,4], y=[3,4,5]; main incorrectly gives y=[4,5,6]. One mutate was verified by execution, not assumed sufficient. |
| Current-relation binding | **Bug fix.** Wildcard casts after rename/projection use the new schema. Filtered whole-table sum of x=[1,2,3], x>1 returns 5; renamed/computed projections also aggregate correctly. Main rebinds the wrong relation/columns. |
| GroupBy | **Expanded behavior.** Computed/aliased keys, multiple keys/reductions, key aliases shadowing input names, no-aggregation grouping and null keys were compared with native Polars. Metrics bind the original input, not a relation mutated with computed keys. Duplicate metric/key names are rejected. Count/len values agree, but their existing signed-Int64 versus Polars-UInt32 dtype discrepancy remains. |
| Naming | **Correct/expanded.** Aliases override names; nested prefix/suffix/uppercase, casts, reductions, literal-left binary expressions and truthy-derived ternary names agree. Dictionary insertion order preserves expression order; duplicate names raise. Generic recursive inference remains separate from translation. |
| Sort | **Bug fix (#213).** Filter/projection/computed-column sorts work; a call-count test confirms the input is translated once. Main fails on the renamed computed projection by replaying its plan. |
| Handler decorator | **Bug fix (#211).** A call-count test passes with both DEBUG and INFO logging; the result is returned without a second handler invocation. |
| Float64/integer whole-table statistics | **Correctness improvement**, not justification for unconditional widening. Removing final Float32 casts preserves precision and avoids overflow; mean([1e39,1e39]) now returns 1e39 instead of DuckDB failing its Float32 cast (SQLite previously exports infinity). |
| Float32 whole-table statistics | **Regression.** Main returns Float32 for mean/var/std on a direct Float32 input; the branch returns Float64. Equivalent expression-level widening already existed on main. |

### Numeric findings

For native Polars on both tested versions: Float32 mean/median/var/std/quantile produce Float32; Float64 produces Float64; integer mean/median/var/std/quantile produce Float64. Sum/min/max preserve the tested Float32/Float64/Int32 input dtype.

On SQLite and DuckDB, the shared mean/var/std handlers generate Float64 for all three numeric input categories. With explicit expression casts (avoiding the existing scan-schema width loss), Float32 min/max preserve Float32; sum/median/quantile widen to Float64 in Ibis. SQLite cannot compile median or quantile. Int32 sum widens to Int64 in Ibis. Whole-table and expression-level results otherwise agree with each other on the branch.

Minimal Float32 reproduction:

```python
lf = pl.DataFrame({"x": pl.Series([1, 2, 4], dtype=pl.Float32)}).lazy()
lf.mean()  # Also var(), std().
```

DuckDB mean: native/main `Float32: 2.3333332538604736`; branch `Float64: 2.3333333333333335`. On [0.1,0.2,0.4] explicitly cast to Float32, branch mean is 0.23333333681027094 versus native Float32 0.23333333432674408. This is observable dtype/rounding behavior, not just a display difference. Casting everything back to Float32 would reintroduce the Float64/integer bug.

Large-value probes also show ordinary backend floating-reduction differences of a few last bits; no blanket tolerance increases were made. Int32 sum overflow differs materially: [2147483647,1,1] sums to -2147483647 in Polars but 2147483649 through the widened Ibis reduction. Width promotion is pre-existing, not a new dispatch regression.

## C. Verified defects and unsupported behavior

| Severity | Function/root cause | Reproduction/result | Disposition |
|---|---|---|---|
| High | `handle_mean/var/std`, scan dtype mapping | Float32 reproduction above; forced Float64 reducers, plus `_get_type()` loses input widths. | **Unfixed.** Preserve source dtype and Float32 results while retaining Float64 for Float64/integer inputs; otherwise explicitly reject unsupported cases. Strict dtype tests added. |
| High | `handle_quantile` | `[1,2,3,4]`, quantile(0.5): Polars nearest=3, DuckDB linear=2.5; quantile(0.25): 2 versus 1.75. | **Pre-existing, unfixed.** Implement nearest faithfully or reject it. Existing tolerance=0.5 masks this defect and is not evidence of correctness. |
| Medium | `apply_select_expr` | Literal-only select returns input-height rows (zero on empty input), instead of one. Also wrong on main. | **Locally fixed:** relation-free scalar projection uses a one-row scalar relation, including filtered-empty input. |
| High | selector `_columns` | `cs.by_dtype([pl.Int64,pl.String]) | cs.decimal()` selects integers/strings natively but integers/floats on the branch. Arbitrary `AnyOf` was treated as all numeric. | **Locally fixed:** accept only the exact supported numeric union; reject other unions. The broad assumption existed in main's FillNull specialization but is newly exposed by Select. |
| Medium | selector `_columns` | `cs.numeric()` and `pl.all().exclude('s')` serialize differently from the patterns the branch recognizes. | **Locally fixed:** Numeric tag and non-strict exclusions; strict missing drops still fail, ordinary non-strict missing exclusions agree with Polars. |
| Medium | `handle_group_by` | `.agg(pl.col('v') + 1)` requires list collection, not a scalar metric. Original branch leaks an Ibis signature error; no silently wrong collection observed. | **Locally explicit NotImplementedError** for column-shaped metrics. List aggregation is deferred feature work. |
| Medium | GroupBy selector context | `lf.group_by('g').agg(pl.all().sum())`: Polars excludes g; branch includes it and rejects a duplicate key/metric. | **Unsupported, not silent.** Added expected-conversion-failure scenario. Defer context-aware expansion rather than inventing a selector engine here. |
| Medium | `handle_sum` | Empty/all-null sum: Polars=0, main/branch=NULL; also affects aggregate broadcasting. | **Locally fixed:** shared sum uses `fill_null(0)`. Existing defect; recommend a separate bugfix. Exact SQL snapshots/doc example updated, not weakened. |
| Low | `apply_select_expr` | `.select([])` is representable as native (0,0); main returns the input, branch emits invalid empty SELECT SQL. | **Locally rejected explicitly**, rather than claiming support. |
| Low | `expand_expr` | `lf.select(pl.all() + pl.all())` is ordinary valid Polars but contains two selector occurrences. | **Unsupported explicitly**, also unsupported on main. Generalized expansion deferred. |

No additional naming, sibling-binding or computed-key-binding defect was verified. Whole-table var/std support only ddof=1, unchanged. SQLite median/quantile limitations and existing dtype-width/count limitations remain backend/feature gaps, not regressions to conceal.

## D. Scope recommendation

- **Keep in deduplication:** separate value dispatcher, generic name inference, expansion/named-expression helpers, correct Select broadcasting, original-input HStack binding, current-relation stats binding, and the local selector/rejection guards.
- **Revise before accepting:** Float32 statistical typing; retain the Float64/integer precision improvement, not the old unconditional result cast. Resolve or explicitly reject nearest quantile rather than relying on a large tolerance.
- **Split as independent bugfixes where practical:** #211 decorator, #213 Sort, sibling-HStack semantics, literal-only cardinality, empty/all-null sum. They need not be redesigned, but are semantic changes beyond deduplication.
- **Defer as feature work:** list aggregation, GroupBy wildcard context, multiple-selector expansion, zero-column relational representation, comprehensive dtype/overflow support. Keep unsupported behavior explicit.

## E. Local artifacts

Local modifications: `src/polars_to_ibis/_parse/{selectors,table_handlers,value_handlers}.py`, the SQL doc example in `src/polars_to_ibis/__init__.py`, parser scenarios/tests, and two SQL snapshot helpers in `tests/config_split.py`. The changes are committed separately from the audited HEAD on `p2i-single-value-dispatch-validation`.

Evidence directory: `/tmp/p2i-validation/`. Important files: `baseline[-all]-136/137.log`, `main-all-136/137.log`, `new-tests-before-fixes.log`, `sum-before-fix.log`, `final-136/137.log`, `final-ci-136/137.log`, `precommit-final.log`, original `branch.diff`, and `local-fixes.diff`. JSONL probes compare native/translated schemas and values on the branch and main (`probe-*`, `numeric-*`, `plain-stats-*`, `edges-*`). Probes are exploratory artifacts, not a replacement test harness; regression coverage uses the existing backend/exporter framework.
