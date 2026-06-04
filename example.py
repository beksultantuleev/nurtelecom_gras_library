"""
Runnable example for nurtelecom_gras_library.

Walks through the common flow:
    1. pull credentials from Vault (AppRole),
    2. open an Oracle connection from those credentials,
    3. run a query into a pandas DataFrame,
    4. (optional) create a table from the DataFrame and load it.

Run it with credentials supplied via environment variables (recommended) ::

    export VAULT_LINK_URL=https://vault.com
    export VAULT_ROLE_ID=...           # AppRole role id
    export VAULT_SECRET_ID=...         # AppRole secret id
    export PATH_TO_SECRET_VLT=path/to/secret
    export MOUNT_POINT_VLT=your_kv_mount
    python example.py

...or edit the values directly in main() below.
"""

import logging

from nurtelecom_gras_library import (
    get_all_cred_dict,
    get_db_connection,
    make_table_query_from_pandas,
)

# The library logs instead of printing; show INFO so you can see progress.
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("example")


# ---- Configuration -------------------------------------------------------
# Leave these as None to read from the environment variables shown above,
# or fill them in here. NEVER commit a real secret_id to source control.
VAULT_URL = None        # e.g. "https://vault.example.com"
ROLE_ID = None          # AppRole role id
SECRET_ID = None        # AppRole secret id
SECRET_PATH = None      # e.g. "path/to/secret"
MOUNT_POINT = None      # e.g. "your_kv_mount"

DB_USER = "myuser"      # combined with DB_NAME to look up the password key
DB_NAME = "DWH"         # e.g. resolves keys MYUSER_DWH, DWH_IP, DWH_SERVICE_NAME, DWH_PORT


def main():
    # 1. Credentials from Vault (AppRole). Any None falls back to the env var.
    all_cred = get_all_cred_dict(
        vault_url=VAULT_URL,
        role_id=ROLE_ID,
        secret_id=SECRET_ID,
        path_to_secret=SECRET_PATH,
        mount_point=MOUNT_POINT,
    )
    logger.info("Loaded %d credential keys from Vault", len(all_cred))

    # 2. Open a connection (context manager releases the engine/pool on exit).
    with get_db_connection(DB_USER, DB_NAME, all_cred) as db:
        # 3. Simple query into a DataFrame.
        df = db.get_data("SELECT 1 AS one FROM dual")
        logger.info("Query returned %d row(s):\n%s", len(df), df)

        # 4. (Optional) create a table from a DataFrame and load it.
        #    Uncomment to try against a writable schema.
        #
        # ddl = make_table_query_from_pandas(df, "my_demo_table")
        # db.execute(ddl)
        # db.upload_pandas_df_to_oracle(df, "my_demo_table")
        # logger.info("Created and loaded my_demo_table")


if __name__ == "__main__":
    main()
