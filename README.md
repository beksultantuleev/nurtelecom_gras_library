# nurtelecom_gras_library

Official Python toolkit for the NurTelecom GRAS data team. It bundles the
everyday building blocks for data work into one package:

- **Oracle access** — query into pandas, bulk insert/upsert, export to file
- **Geospatial** — read WKT/SDO geometries straight into a GeoDataFrame
- **Secrets** — pull credentials from HashiCorp Vault
- **SQL generation** — build `CREATE TABLE` DDL (with partitioning) from a DataFrame
- **Notifications** — Telegram, SMS, and e-mail (plain + HTML)
- **Jira** — create/update issues and Service Desk requests
- **Tableau** — export views to PNG / PDF / CSV / XLSX

## Installation

```bash
pip install nurtelecom_gras_library
```

Spatial features (`OracleGeoDataImporter`, `voronoi_split`) need extra,
heavier dependencies. Install them only if you need them:

```bash
pip install nurtelecom_gras_library[geo]      # geopandas, shapely, scipy
pip install nurtelecom_gras_library[smb]      # smbprotocol (network shares)
pip install nurtelecom_gras_library[tableau]  # tableauserverclient
```

The core install does **not** pull in `geopandas`/`shapely`/`tableauserverclient`;
you'll get a clear error pointing you to the right extra if you use a feature
that needs one.

---

## Quick start

A complete, runnable version of the flow below lives in
[`example.py`](example.py) — copy it and fill in your details.

```python
import logging
from nurtelecom_gras_library import (
    get_db_connection,
    get_all_cred_dict,
    make_table_query_from_pandas,
)

# Optional: the library logs (doesn't print) — show INFO to see progress.
logging.basicConfig(level=logging.INFO)

# 1. Pull credentials from Vault. AppRole (role_id + secret_id) is recommended;
#    any argument left out is read from the matching env var (see Credentials).
all_cred_dict = get_all_cred_dict(
    vault_url="https://vault.example.com",
    role_id="your-approle-role-id",
    secret_id="your-approle-secret-id",
    path_to_secret="path/to/secret",
    mount_point="your_kv_mount",
)

# 2. Open a connection for a given user + database (context manager closes it).
with get_db_connection("my_login", "DWH", all_cred_dict) as db:
    # 3. Query into a pandas DataFrame.
    df = db.get_data("SELECT 1 AS one FROM dual")

    # 4. Generate and run a CREATE TABLE from the DataFrame, then load it.
    ddl = make_table_query_from_pandas(df=df, table_name="my_new_table")
    db.execute(ddl)
    db.upload_pandas_df_to_oracle(df, "my_new_table")
```

---

## Database access (Oracle)

`get_db_connection(user, database, all_cred_dict=None, geodata=False)` returns an
`OracleDataRetriever` (or an `OracleGeoDataImporter` when `geodata=True`). It looks
up `{USER}_{DATABASE}`, `{DATABASE}_IP`, `{DATABASE}_SERVICE_NAME`, and
`{DATABASE}_PORT` from the credentials dict. You can also instantiate the class
directly:

```python
from nurtelecom_gras_library import OracleDataRetriever

db = OracleDataRetriever(
    user="user", password="pass",
    host="192.168.1.1", port="1521", service_name="DWH",
)
```

Connections are context managers — use `with` to release the engine/pool
automatically (or call `db.close()` yourself):

```python
with get_db_connection("my_login", "DWH", all_cred_dict) as db:
    df = db.get_data("SELECT 1 FROM dual")
```

#### Oracle Client / Thick mode

On import the library tries to initialize **Thick mode** (it loads the Oracle
Client libraries), falling back silently to **Thin mode** if the client isn't
found. Thick mode is required for accounts using older password verifiers —
otherwise you'll see `DPY-3015: password verifier type ... not supported ... in
thin mode`.

If the client isn't on the default loader path you'll get
`DPI-1047: Cannot locate ... Oracle Client library`. Point the library at it via
either:

```bash
export ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient_21_13   # used on import
# or set LD_LIBRARY_PATH before launching Python
```
```python
from nurtelecom_gras_library import enable_thick_mode
enable_thick_mode(lib_dir="/opt/oracle/instantclient_21_13")    # explicit
```

Set `NURTELECOM_THICK_MODE=0` to skip the auto-init and stay in Thin mode.

### Reading

```python
df = db.get_data("SELECT * FROM my_table", remove_na=True, show_logs=True)
```

### Writing

```python
# Bulk insert (batched executemany)
db.upload_pandas_df_to_oracle(df, table_name="my_table")

# Parallel bulk insert using an internal connection pool
db.upload_pandas_df_to_oracle_parallel(df, "my_table", num_threads=4, batch_size=10000)

# MERGE / upsert on key columns (optionally summing some columns)
db.upsert_from_pandas_df(
    df, table_name="my_table",
    list_of_keys=["id"],
    sum_update_columns=["counter"],
)
```

### Exporting & maintenance

```python
db.export_to_file("SELECT * FROM big_table", "out.csv")          # chunked via pandas
db.export_to_file_oracle("SELECT * FROM big_table", "out.csv")   # chunked via oracledb
db.truncate_table("my_table")
db.execute("BEGIN my_proc(); END;")
```

### Geospatial

`OracleGeoDataImporter` (returned by `get_db_connection(..., geodata=True)`)
parses geometry columns into a `geopandas.GeoDataFrame`. Have your query return
geometry as a standard interchange format. **WKB (binary) is preferred** —
compact, fast, and lossless:

```sql
SELECT SDO_UTIL.TO_WKBGEOMETRY(SDO_CS.TRANSFORM(t.geometry, 4326)) AS geometry
FROM my_geo_table t
```

WKT (text) is also supported:

```sql
SELECT SDO_UTIL.TO_WKTGEOMETRY(SDO_CS.TRANSFORM(t.geometry, 4326)) AS geometry
FROM my_geo_table t
```

```python
# geom_format defaults to "auto" (detects WKB bytes vs WKT text per value);
# pass "wkb" or "wkt" to force one.
gdf = geo_db.get_data(query, geom_columns_list=["geometry"], crid="EPSG:4326")
```

NULL geometries become `None`, CLOB/BLOB columns are read correctly, and the
first column in `geom_columns_list` is set as the active geometry (so it can be
named anything).

Geometry can also be written back via the `geometry_cols`/`srid` arguments on the
upload methods (using `SDO_GEOMETRY`).

---

## SQL generation from DataFrames

```python
from nurtelecom_gras_library import make_table_query_from_pandas

ddl = make_table_query_from_pandas(
    df,
    table_name="events",
    list_num_columns=["amount"],
    list_date_columns=["event_date"],
    list_clob_columns=["payload"],
    # Optional partitioning
    partition_column="event_date",
    partition_type="RANGE",
    partition_granularity="MONTH",
    partition_start="2024-01-01",
    partition_end="2025-01-01",
)
```

Supports `RANGE` (manual or interval), `LIST`, and `HASH` partitioning.

---

## Credentials (Vault)

```python
from nurtelecom_gras_library import get_all_cred_dict

# AppRole auth (recommended): role_id + secret_id
creds = get_all_cred_dict(
    vault_url=url, role_id="your-approle-role-id", secret_id="your-approle-secret-id",
    path_to_secret="path/to/secret", mount_point="your_kv_mount",
)

# Token auth
creds = get_all_cred_dict(vault_url=url, vault_token=token, path_to_secret=..., mount_point=...)
```

AppRole takes precedence when both `role_id` and `secret_id` are supplied.
Values are used **as-is (raw)** — `vault_url` must include the scheme
(`https://…`). If any argument is omitted, it is read verbatim from the
corresponding environment variable: `VAULT_LINK_URL`, `VAULT_TKN`,
`VAULT_ROLE_ID`, `VAULT_SECRET_ID`, `PATH_TO_SECRET_VLT`, `MOUNT_POINT_VLT`. The
function raises `ValueError` if no credentials are given and `RuntimeError` if
Vault auth fails. Helpers `pass_encoder` / `pass_decoder` still provide base64
encode/decode if you want them (note: base64 is encoding, **not** encryption).

---

## Notifications

### Telegram

```python
from nurtelecom_gras_library import (
    send_msg_via_telegram, send_photo_via_telegram, send_file_via_telegram,
)

send_msg_via_telegram(token, chat_id, "Hello")
send_photo_via_telegram(token, chat_id, "chart.png", captions="Daily chart")
send_file_via_telegram(token, chat_id, "report.xlsx")
```

`send_telegram_msg(payload, receiver, db)` and `send_sms(payload, receiver, db)`
route messages through Oracle stored procedures using a DB connection.

### E-mail

```python
from nurtelecom_gras_library import send_email, send_email_html

# Plain text + attachments (single path, or a whole directory)
send_email(
    send_to=["a@x.kg", "b@x.kg"], send_from="bot@x.kg",
    subject="Report", host="smtp.host", content="See attached",
    file_to_attach="report.pdf", cc_to="boss@x.kg",
)

# HTML body with a plain-text fallback
send_email_html(
    send_to="a@x.kg", send_from="bot@x.kg", subject="Report",
    host="smtp.host", html="<h1>Hi</h1>", text="Hi",
)
```

`send_to`/`cc_to` accept either a string or a list/tuple of addresses.

---

## Jira

```python
from nurtelecom_gras_library import JiraClient

jira = JiraClient(base_url="https://jira.example", token="PAT")
# or: JiraClient(base_url=..., username=..., password=...)

# Standard projects
key = jira.create_project_issue("OPTM", "Task", {"summary": "New task"})
jira.update_project_issue_fields(key, {"description": "..."})
jira.add_comment_to_issue(key, "Working on it")
jira.add_attachment_to_issue(key, "log.txt")
df = jira.get_issues_df(jira.fetch_all_issues("project = OPTM"), field_map={"Key": "Key", "summary": None})

# Service Desk
req = jira.create_service_desk_request(service_desk_id, request_type_id, fields)
jira.add_comment_to_request(req, "Update", public=True)
```

Includes discovery helpers: `list_available_projects`, `list_issue_types_for_project`,
`get_project_issue_fields`, `get_request_types`, `get_request_fields`,
`check_portal_access`.

---

## Tableau

```python
from nurtelecom_gras_library import TableauServerManager

tab = TableauServerManager(login="user", password="pass", host="https://tableau.host")

# Export a view directly from its browser URL (PNG/PDF/CSV/XLSX).
# Resolves the workbook from the URL slug — no full-site enumeration needed.
status, file_path = tab.export_view(
    "https://tableau.host/#/views/MyWorkbook/MyView", format="png",
)
```

The server REST API version is auto-detected by default; pin it with
`server_version="3.9"` if needed. Exported files are written to an
`exported_files/` folder.

---

## Utilities

| Function | Purpose |
| --- | --- |
| `measure_time` | Decorator that prints a function's runtime |
| `get_list_of_objects(path, is_dir=False)` | List files (or dirs) in a path |
| `get_a_copy(src, dst)` | Copy a file |
| `value_extractor(pattern, path)` | Extract a numeric value from a text file |
| `merge_clob_maker(string, num_of_charr=25000)` | Split a long string into `TO_CLOB(...)` chunks for Oracle |
| `voronoi_split(poly, coords, ...)` | Voronoi partition of a polygon *(requires `[geo]`)* |
| `register_smb_session(user, password, server)` | Open an SMB session *(requires `[smb]`)* |

---

## License

MIT License — see [LICENSE](LICENSE).

## Contact

For questions or support, contact the NurTelecom GRAS team.
