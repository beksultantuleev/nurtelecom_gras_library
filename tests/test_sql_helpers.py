import pandas as pd
import pytest

from nurtelecom_gras_library import make_table_query_from_pandas
from nurtelecom_gras_library.additional_functions import merge_clob_maker
from nurtelecom_gras_library.OracleDataRetriever import _validate_identifier


def test_make_table_query_basic_types():
    df = pd.DataFrame({"id": [1], "name": ["a"], "ts": [pd.Timestamp("2024-01-01")]})
    ddl = make_table_query_from_pandas(
        df, "my_table",
        list_num_columns=["id"],
        list_timestamp_columns=["ts"],
    )
    assert ddl.startswith("CREATE TABLE my_table (")
    assert "id \t number" in ddl
    assert "ts \t timestamp" in ddl
    assert "name \t varchar2(500)" in ddl
    assert ddl.rstrip().endswith(")")


def test_make_table_query_range_partition():
    df = pd.DataFrame({"event_date": [pd.Timestamp("2024-01-01")], "val": [1]})
    ddl = make_table_query_from_pandas(
        df, "events",
        list_date_columns=["event_date"],
        list_num_columns=["val"],
        partition_column="event_date",
        partition_type="RANGE",
        partition_granularity="MONTH",
        partition_start="2024-01-01",
        partition_end="2024-04-01",
    )
    assert "PARTITION BY RANGE (event_date)" in ddl
    assert "VALUES LESS THAN" in ddl


def test_merge_clob_maker_splits_and_concats():
    out = merge_clob_maker("abcdef", num_of_charr=2)
    assert out == "to_clob('ab') || to_clob('cd') || to_clob('ef')"


def test_merge_clob_maker_single_chunk():
    assert merge_clob_maker("abc", num_of_charr=100) == "to_clob('abc')"


@pytest.mark.parametrize("name", ["my_table", "SCHEMA.TABLE", "t1$#_x"])
def test_validate_identifier_accepts_valid(name):
    assert _validate_identifier(name) == name


@pytest.mark.parametrize("name", [
    "table; DROP TABLE x",
    "1bad",
    "a b",
    "tbl--",
    "",
    "a.b.c",
])
def test_validate_identifier_rejects_injection(name):
    with pytest.raises(ValueError):
        _validate_identifier(name)
