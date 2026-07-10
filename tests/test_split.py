import json
from pathlib import Path
from tempfile import NamedTemporaryFile

import ibis
import opendp.prelude as dp
import polars as pl


def split(polars_plan):
    # TODO: Generalize
    # TODO: Don't modify original
    # TODO: Return parameters for the plugin as well
    # TODO: Move into public interface
    polars_plan["Select"]["expr"][0]["Function"] = polars_plan["Select"]["expr"][0][
        "Function"
    ]["input"][0]["Function"]


def test_split_lazyframe():
    # Example copied from opendp:
    dp.enable_features("contrib")
    context = dp.Context.compositor(
        data=pl.scan_csv(
            dp.examples.get_france_lfs_path(),
            ignore_errors=True,
        ),
        privacy_unit=dp.unit_of(contributions=36),
        privacy_loss=dp.loss_of(epsilon=1.0),
        split_evenly_over=5,
        margins=[
            dp.polars.Margin(max_length=150_000 * 36),
        ],
    )
    query_work_hours = (
        # 99 represents "Not applicable"
        context.query().filter(pl.col("HWUSUAL") != 99.0)
        # compute the DP sum
        .select(pl.col.HWUSUAL.cast(int).fill_null(35).dp.sum(bounds=(0, 80)))
    )
    # More OpenDP serialization should help here.
    # See https://github.com/opendp/polars-to-ibis/pull/93
    with NamedTemporaryFile(mode="w") as temp:
        # TODO: Don't write to filesystem.
        query_work_hours._ldf.serialize_json(temp.name)
        polars_plan = json.loads(Path(temp.name).read_text())

    split(polars_plan)

    from polars_to_ibis import _get_input_schema

    input_schema = _get_input_schema(polars_plan)
    table_name = "placeholder"
    ibis_table = ibis.table(input_schema, name=table_name)  # type: ignore

    from polars_to_ibis._parse.table_handlers import update_polars_to_ibis

    updated_table = update_polars_to_ibis(
        polars_plan=polars_plan,
        table=ibis_table,
    )

    def norm_sql(sql: str):
        import re

        return re.sub(r"\s+", " ", sql).replace('"', "").strip()

    sql = norm_sql(updated_table.to_sql())
    expected_sql = norm_sql("""
        SELECT COALESCE(CAST(t0.HWUSUAL AS BIGINT), 35) AS HWUSUAL
        FROM placeholder AS t0
        WHERE t0.HWUSUAL <> 99.0
    """)
    assert sql == expected_sql
