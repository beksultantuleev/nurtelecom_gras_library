# Changelog

All notable changes to this project are documented here. This project adheres
to [Semantic Versioning](https://semver.org/).

## [2.3.1]

### Changed
- Restored automatic Thick-mode initialization on import (with a silent
  Thin-mode fallback), so accounts using older password verifiers connect
  without an explicit call — matching pre-2.3.0 behavior. Now configurable via
  the `ORACLE_CLIENT_LIB_DIR` env var, `enable_thick_mode(lib_dir=...)`, and
  `NURTELECOM_THICK_MODE=0` to opt out.

### Added
- README troubleshooting note for Thick mode / `DPY-3015` / `DPI-1047`.

## [2.3.0]

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
- `get_all_cred_dict` supports Vault **AppRole** auth (`role_id` + `secret_id`,
  or an `approle={role_id: secret_id}` dict) alongside token auth.
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
  mode was made opt-in via `enable_thick_mode()` instead of running on import.
  (Reverted in 2.3.1 — see above.)
- SQL statements that took user input (`send_sms`, `send_telegram_msg`) now use
  bind parameters; `truncate_table` validates the table identifier.
- `get_all_cred_dict` now uses raw credential values (no base64 decoding); its
  environment variables (`VAULT_LINK_URL`, `VAULT_TKN`, `VAULT_ROLE_ID`,
  `VAULT_SECRET_ID`, `PATH_TO_SECRET_VLT`, `MOUNT_POINT_VLT`) are read verbatim,
  and it raises on missing creds / failed auth instead of returning `None`.

### Removed
- Obsolete `setup.py`, `setup.cfg`, and `test_toml.py`.
- Deprecated `make_table_query_from_pandas_old` and the slow, row-by-row
  `upload_pandas_df_to_oracle_row`.
- Unused `transliterate` dependency.

## [2.2.0]

- Previous release (parallel upload, Jira client, Tableau manager).
