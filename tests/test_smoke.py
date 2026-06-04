"""Import smoke tests — these alone would have caught the 2.2.0 import crashes."""


def test_package_imports():
    import nurtelecom_gras_library as lib
    assert isinstance(lib.__all__, list) and lib.__all__


def test_public_api_is_importable():
    import nurtelecom_gras_library as lib
    for name in lib.__all__:
        assert hasattr(lib, name), f"{name} listed in __all__ but not importable"


def test_key_symbols_present():
    from nurtelecom_gras_library import (  # noqa: F401
        get_db_connection,
        OracleDataRetriever,
        OracleGeoDataImporter,
        JiraClient,
        TableauServerManager,
        make_table_query_from_pandas,
        send_email,
        send_email_html,
    )
