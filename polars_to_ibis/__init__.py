"""Convert Polars plans to Ibis tables"""

from pathlib import Path

import ibis  # pyright: ignore [reportMissingTypeStubs]
import polars as pl

__version__ = (Path(__file__).parent / "VERSION").read_text().strip()

# Polars 1.32 is needed to support OpenDP 0.14.1:
_min_polars = "1.32.0"
# TODO: When we drop Polars 1.32 support, we could simplify things.
# _min_polars = "1.33.0"
_max_polars = "1.34.0"


class PolarsToIbisWarning(Warning):
    pass


def _warn(message: str):  # pragma: no cover
    # It's hard to remember to use the wrapping class,
    # so do it by default,
    # and keep "warn" out of the global namespace.
    from warnings import warn

    warn(PolarsToIbisWarning(message))


def _check_version():
    if not (
        _min_polars.split(".")  # Oldest supported
        <= pl.__version__.split(".")  # Installed
        <= _max_polars.split(".")  # Newest supported
    ):
        _warn(  # pragma: no cover
            f"Polars {pl.__version__} has not been tested! "
            f"Try {_min_polars} to {_max_polars}."
        )


def convert_polars_to_ibis(lf: pl.LazyFrame, table_name: str) -> ibis.Table:
    from polars_to_ibis.parse import update_polars_to_ibis
    from polars_to_ibis.serialize import Serialization

    _check_version()

    # NOTE: Tests fail if the order of serialize() and collect_schema() is switched.
    # TODO: Understand whether the schema or the plan is changing.

    polars_plan = Serialization(lf)._serial  # type: ignore
    polars_schema = lf.collect_schema()

    ibis_schema = ibis.expr.schema.Schema.from_polars(polars_schema)
    ibis_table = ibis.table(ibis_schema, name=table_name)  # type: ignore

    return update_polars_to_ibis(
        polars_plan=polars_plan,
        table=ibis_table,
    )
