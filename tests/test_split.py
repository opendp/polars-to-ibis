import json
from pathlib import Path
from tempfile import NamedTemporaryFile

import ibis
import opendp.prelude as dp
import polars as pl


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
    with NamedTemporaryFile(mode="w") as temp:
        # TODO: Don't write to filesystem.
        query_work_hours._ldf.serialize_json(temp.name)
        polars_plan = json.loads(Path(temp.name).read_text())
        # breakpoint()

    from polars_to_ibis import _get_input_schema

    input_schema = _get_input_schema(polars_plan)
    table_name = "placeholder"
    ibis_table = ibis.table(input_schema, name=table_name)  # type: ignore

    from polars_to_ibis._parse.table_handlers import update_polars_to_ibis

    updated_table = update_polars_to_ibis(
        polars_plan=polars_plan["Select"]["input"],
        table=ibis_table,
    )

    assert "UnboundTable" in str(updated_table)
