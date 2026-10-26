"""Exercise plugin splitting with a small native plugin, without OpenDP."""

import os
import subprocess
import sys
from pathlib import Path

import ibis
import polars as pl
import pytest
from polars.plugins import register_plugin_function

from polars_to_ibis import scan_database, split_polars_on_ffi
from polars_to_ibis._serialize import serialize

from .utils import backend_names, get_connection


@pytest.fixture(scope="session")
def simple_plugin_path():
    source = Path(__file__).with_name("simple_plugin")
    target = Path(os.environ.get("CARGO_TARGET_DIR", source / "target")).resolve()
    subprocess.run(
        ["cargo", "build", "--locked", "--manifest-path", str(source / "Cargo.toml")],
        check=True,
        env={
            **os.environ,
            "PYO3_PYTHON": sys.executable,
            "CARGO_TARGET_DIR": str(target),
        },
    )
    prefix, suffix = (
        ("", "dll")
        if sys.platform == "win32"
        else (
            "lib",
            "dylib" if sys.platform == "darwin" else "so",
        )
    )
    library = target / "debug" / f"{prefix}polars_to_ibis_test_plugin.{suffix}"
    assert library.is_file()
    return library


def add_offset(expression, plugin_path, amount=7):
    return register_plugin_function(
        plugin_path=plugin_path,
        function_name="add_offset",
        args=expression,
        kwargs={"amount": amount},
        is_elementwise=True,
    )


@pytest.mark.parametrize("backend_name", backend_names)
def test_split_native_plugin(simple_plugin_path, backend_name):
    table_name = "simple_plugin_test"
    values = pl.DataFrame({"value": [1, 2, 3, 4]}, schema={"value": pl.Int64})
    expected = (
        values.lazy()
        .select(add_offset(pl.col("value").sum(), simple_plugin_path))
        .collect()
    )
    connection = get_connection(values, table_name, getattr(ibis, backend_name))
    try:
        query = scan_database(connection, table_name).select(
            add_offset(pl.col("value").sum(), simple_plugin_path)
        )
        original_parameters = serialize(query)["Select"]["expr"][0]["Function"][
            "function"
        ]["FfiPlugin"]
        table, parameters = split_polars_on_ffi(query, table_name, connection)
        assert parameters == [original_parameters]
        assert parameters[0]["symbol"] == "add_offset"
        assert Path(parameters[0]["lib"]).resolve() == simple_plugin_path.resolve()
        aggregate_result = connection.to_polars(table)
        assert aggregate_result.to_dict(as_series=False) == {"value": [10]}

        # The splitter preserves the opaque kwargs; Polars runs the plugin.
        actual = (
            aggregate_result.lazy()
            .select(
                register_plugin_function(
                    plugin_path=Path(parameters[0]["lib"]),
                    function_name=parameters[0]["symbol"],
                    args=pl.col("value"),
                    kwargs={"amount": 7},
                    is_elementwise=True,
                )
            )
            .collect()
        )
        assert actual.equals(expected)
        assert actual.to_dict(as_series=False) == {"value": [17]}
    finally:
        connection.drop_table(table_name)
        connection.disconnect()
