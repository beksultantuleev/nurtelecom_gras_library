# Changelog

All notable changes to this project are documented here. This project adheres
to [Semantic Versioning](https://semver.org/).

## [2.2.1]

### Fixed
- **Import-breaking bugs.** Removed an import-time call to `register_smb_session()`
  that crashed on `import`; `register_smb_session` now uses its arguments and
  imports `smbclient` lazily (via the `[smb]` extra).
- `get_all_cred_dict` no longer evaluates Vault environment variables at import
  time; missing credentials are resolved (and raise) only when the function is
  called.
- Added the missing imports used by `voronoi_split` and the table-DDL helpers.
- `numpy` is now a declared dependency.

### Added
- **Optional extras.** `geopandas`/`shapely`/`scipy` moved to a `[geo]` extra and
  `smbprotocol` to a `[smb]` extra, so the core install is lightweight. A `[dev]`
  extra provides the test toolchain.
- `OracleGeoDataImporter.get_data` gained a `geom_format` parameter
  (`"auto"` | `"wkb"` | `"wkt"`); **WKB** is now the preferred, lossless path.
- Connection objects are now context managers (`with get_db_connection(...) as db:`)
  and expose `close()`.
- `send_email` gained an optional `html` body plus `port`/`use_tls`/auth options
  and `Date`/`Message-ID` headers.
- `py.typed` marker (the package now ships type information).
- Test suite (`pytest`) and GitHub Actions CI.

### Changed
- Replaced `print()` calls with the standard `logging` module across the library.
- `OracleGeoDataImporter` reads CLOB/BLOB geometries correctly and tolerates NULL
  geometries; the first listed geometry column is set as the active geometry.
- The Oracle connection pool is now created lazily (only when needed), and Thick
  mode is opt-in via `enable_thick_mode()` instead of running on import.
- SQL statements that took user input (`send_sms`, `send_telegram_msg`) now use
  bind parameters; `truncate_table` validates the table identifier.

### Removed
- Obsolete `setup.py`, `setup.cfg`, and `test_toml.py`.
- Deprecated `make_table_query_from_pandas_old` and the slow, row-by-row
  `upload_pandas_df_to_oracle_row`.
- Unused `transliterate` dependency.

## [2.2.0]

- Previous release (parallel upload, Jira client, Tableau manager).
