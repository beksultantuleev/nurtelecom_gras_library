import os
import oracledb
import pandas as pd
import numpy as np
import csv
import json
import re
import logging
from urllib.parse import quote_plus
from sqlalchemy.engine import create_engine
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor, as_completed
from nurtelecom_gras_library.additional_functions import measure_time

logger = logging.getLogger(__name__)

# Valid Oracle identifier (optionally schema-qualified), used to guard against
# SQL injection where a value cannot be bound (e.g. table names in DDL).
_IDENTIFIER_RE = re.compile(r'^[A-Za-z][A-Za-z0-9_$#]*(\.[A-Za-z][A-Za-z0-9_$#]*)?$')


def enable_thick_mode(lib_dir=None):
    """Enable python-oracledb Thick mode (loads the Oracle Client libraries).

    Thick mode is required to connect to accounts that use older password
    verifiers (otherwise Thin mode raises ``DPY-3015``). Safe to call once at
    application startup; raises ``DatabaseError`` (``DPI-1047``) if the Oracle
    Client libraries cannot be located.

    :param lib_dir: Path to the Oracle Client library directory. If ``None``,
        the loader's default search path (``LD_LIBRARY_PATH`` / ``ldconfig`` on
        Linux, ``PATH`` on Windows) is used.
    """
    oracledb.init_oracle_client(lib_dir=lib_dir)


def _auto_init_thick_mode():
    """Best-effort Thick-mode initialization at import (pre-2.3.0 behavior).

    Many GRAS accounts use older password verifiers that only work in Thick
    mode, so this attempts to load the Oracle Client automatically. It uses the
    ``ORACLE_CLIENT_LIB_DIR`` environment variable as the client directory when
    set. Any failure (e.g. client not installed) falls back silently to Thin
    mode. Set ``NURTELECOM_THICK_MODE=0`` to skip this, or call
    :func:`enable_thick_mode` explicitly with a ``lib_dir``.
    """
    if os.getenv("NURTELECOM_THICK_MODE", "1") == "0":
        return
    try:
        enable_thick_mode(lib_dir=os.getenv("ORACLE_CLIENT_LIB_DIR") or None)
        logger.debug("Oracle Client initialized (Thick mode).")
    except Exception as e:
        logger.debug("Thick mode unavailable (%s); using Thin mode.", e)


# Mirror the pre-2.3.0 behavior: try Thick mode on import, fall back to Thin.
_auto_init_thick_mode()


def _validate_identifier(name):
    """Return ``name`` if it is a safe Oracle identifier, else raise ValueError."""
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe or invalid SQL identifier: {name!r}")
    return name


class OracleDataRetriever():

    def __init__(self, user: str, password: str, host: str,
                 port: str = '1521', service_name: str = 'DWH') -> None:
        """Create an Oracle connector.

        Builds the SQLAlchemy engine URL and DSN. The SQLAlchemy engine and the
        oracledb connection pool (used only by the parallel upload path) are
        created lazily on first use and released by :meth:`close`. The object is
        usable as a context manager::

            with OracleDataRetriever(...) as db:
                df = db.get_data("SELECT 1 FROM dual")

        :param user: Database user.
        :param password: Database password.
        :param host: Database host/IP.
        :param port: Listener port (default '1521').
        :param service_name: Oracle service name (default 'DWH').
        """
        self.host = host
        self.port = port
        self.service_name = service_name
        self.user = user
        self.password = password
        self.dsn = oracledb.makedsn(self.host, self.port, service_name=self.service_name)
        # URL-encode credentials so special characters don't break the DSN.
        self.engine_url = (
            f'oracle+oracledb://{quote_plus(self.user)}:'
            f'{quote_plus(self.password)}@{self.dsn}'
        )
        self._engine = None
        self._pool = None

    def get_pool(self):
        """Return the oracledb connection pool, creating it on first use.

        :return: The connection pool, or ``None`` if it could not be created.
        """
        if self._pool is None:
            try:
                self._pool = oracledb.create_pool(
                    user=self.user, password=self.password, dsn=self.dsn,
                    min=4, max=12, increment=1,
                )
                logger.info("Oracle connection pool initialized.")
            except oracledb.Error as e:
                logger.warning(
                    "Failed to create connection pool; parallel uploads "
                    "will be unavailable. Error: %s", e)
                self._pool = None
        return self._pool

    # Backward-compatible attribute: some callers referenced ``.pool`` directly.
    @property
    def pool(self):
        return self.get_pool()

    def close(self):
        """Dispose the SQLAlchemy engine and close the connection pool."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        if self._pool is not None:
            try:
                self._pool.close()
            except oracledb.Error as e:
                logger.warning("Error closing connection pool: %s", e)
            self._pool = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def get_engine(self):
        """
        Creates and returns a SQLAlchemy engine for database connections.

        Usage:
        engine = database_connector.get_engine()
        conn = engine.connect()
        # Perform database operations
        conn.close()

        Note: Remember to close the connection after use.
        """
        if self._engine is None:
            try:
                self._engine = create_engine(
                    self.engine_url, echo=False, future=True)
            except Exception as e:
                logger.error("Error creating engine: %s", e)
                raise
        return self._engine

    def get_data(self, query: str, remove_column=None, remove_na: bool = False, show_logs: bool = False) -> pd.DataFrame:
        """
        Retrieve data from the database based on a SQL query.

        :param query: SQL query for data retrieval
        :param remove_column: Columns to remove from the resulting DataFrame, defaults to None
        :param remove_na: Flag to indicate if NA values should be dropped, defaults to False
        :param show_logs: Flag to indicate if logs should be shown, defaults to False
        :return: pandas DataFrame containing the retrieved data
        """
        remove_column = remove_column or []
        try:
            query = text(query)
            engine = self.get_engine()

            with engine.connect() as conn:
                data = pd.read_sql(query, con=conn)
                data.columns = data.columns.str.lower()
                if remove_column:
                    data.drop(columns=remove_column, inplace=True)
                if remove_na:
                    data.dropna(inplace=True)

            if show_logs:
                logger.info("Retrieved %d rows; head:\n%s", len(data), data.head(5))
            return data

        except Exception as e:
            logger.error("Error during data retrieval: %s", e)
            raise

    @measure_time
    def export_to_file(self, query, path, is_csv=True, sep=',', encoding='utf-8'):
        """
        encoding='utf-8-sig' if Cyrillic 
        Export data from a database query to a file in CSV or JSON format.

        :param query: SQL query to export data
        :param path: File path to export the data
        :param is_csv: Boolean flag to determine if the output should be CSV (default) or JSON
        :param sep: Separator for CSV file, defaults to ';'
        """
        try:
            query = text(query)
            engine = self.get_engine()

            with engine.connect() as conn, open(path, 'w') as f:
                for i, partial_df in enumerate(pd.read_sql(query, conn, chunksize=100000)):
                    logger.info('Writing chunk "%s" to "%s"', i, path)
                    if is_csv:
                        partial_df.to_csv(
                            f, index=False, header=(i == 0), sep=sep, encoding=encoding)
                    else:
                        if i == 0:
                            partial_df.to_json(f, orient='records', lines=True)
                        else:
                            partial_df.to_json(
                                f, orient='records', lines=True, header=False)

        except Exception as e:
            logger.error("Error during export: %s", e)
            raise

    @measure_time
    def export_to_file_oracle(self, query: str, path: str, is_csv: bool = True,
                              sep: str = ',', encoding: str = 'utf-8', chunk_size: int = 1000) -> None:
        """
        Export data from an Oracle database query to a file using oracledb and csv module, with progress tracking.

        :param query: SQL query to export data
        :param path: File path to export the data
        :param is_csv: Boolean flag to determine if the output should be CSV (default) or JSON
        :param sep: Separator for CSV file, defaults to ','
        :param encoding: Encoding format to be used for writing the file, defaults to 'utf-8'
        :param chunk_size: Number of rows to process at a time, default is 1000
        """
        try:
            with oracledb.connect(user=self.user, password=self.password, dsn=self.dsn) as connection:
                cursor = connection.cursor()

                cursor.execute(query)

                with open(path, 'w', newline='', encoding=encoding) as f:
                    writer = None
                    if is_csv:
                        writer = csv.writer(f, delimiter=sep)

                    if is_csv:
                        column_names = [col[0] for col in cursor.description]
                        writer.writerow(column_names)

                    row_count = 0
                    chunk_count = 0

                    while True:
                        rows = cursor.fetchmany(chunk_size)
                        if not rows:
                            break

                        chunk_count += 1
                        row_count += len(rows)

                        if is_csv:
                            writer.writerows(rows)
                        else:
                            for row in rows:
                                json_data = {
                                    column_names[i]: value for i, value in enumerate(row)}
                                f.write(json.dumps(json_data) + '\n')

                        logger.info(
                            "Chunk %d written, %d rows in this chunk, %d total rows written.",
                            chunk_count, len(rows), row_count)

            logger.info("Export complete. %d rows written in %d chunks.",
                        row_count, chunk_count)

        except oracledb.DatabaseError as e:
            error, = e.args
            logger.error("Database error during export: %s", error.message)
            raise

    def truncate_table(self, table_name):
        """
        Truncate a table in the database. Be very careful with this function as
        it will remove all data from the specified table.

        :param table_name: Name of the table to be truncated
        :type table_name: str
        """

        # Validate or sanitize the table_name if necessary
        # (e.g., check if it's a valid table name, exists in the database, etc.)

        # Table names cannot be bound as parameters, so validate the identifier
        # to prevent SQL injection.
        _validate_identifier(table_name)
        try:
            self.execute(query=f"TRUNCATE TABLE {table_name}")
            logger.info("Table '%s' truncated successfully.", table_name)
        except Exception as e:
            logger.error("Error occurred while truncating table '%s': %s", table_name, e)
            raise

    def final_query_for_insertion(self, table_name, payload=None, columns_to_insert=None):
        """Build an anonymous PL/SQL block that inserts one row and commits.

        :param table_name: Target table name.
        :param payload: Comma-separated VALUES expression for the row.
        :param columns_to_insert: Optional comma-separated column list; if None,
            the INSERT omits the column list and relies on table column order.
        :return: A PL/SQL ``BEGIN ... INSERT ... COMMIT; END;`` string.
        """
        # place_holder = insert_from_pandas(data, counter, list_of_columns_to_insert)

        query = f'''        
                BEGIN
                    INSERT INTO {table_name} ({columns_to_insert})
                        VALUES({payload});
                    COMMIT;
                END;
            ''' if columns_to_insert != None else f'''        
                BEGIN
                    INSERT INTO {table_name}
                        VALUES({payload});
                    COMMIT;
                END;
            '''
        return query

    def execute(self, query, params=None, verbose=False):
        """Execute a non-returning SQL/PL-SQL statement (DDL, DML, PL/SQL block).

        Runs inside a transaction that commits on success. Pass ``params`` to
        use bind variables instead of string interpolation (recommended for any
        user-supplied values).

        :param query: The SQL or PL/SQL statement to execute.
        :param params: Optional dict of bind parameters (e.g. ``{"id": 1}``).
        :param verbose: If True, log a success message.
        """
        engine = self.get_engine()
        try:
            # engine.begin() opens a transaction and commits on clean exit.
            with engine.begin() as conn:
                conn.execute(text(query), params or {})
            if verbose:
                logger.info('Query executed successfully.')
        except Exception as e:
            logger.error('Error during query execution: %s', e)
            raise


    # --- NEW: Improved Parallel Upload (Fixes DPI-1001 & AttributeError) ---
    @measure_time
    def upload_pandas_df_to_oracle_parallel(self, pandas_df: pd.DataFrame, table_name: str,
                                            geometry_cols: list = None, srid: int = 4326,
                                            num_threads: int = 4, batch_size: int = 10000) -> None:
        """
        Parallel upload with internal batching to prevent 'DPI-1001: out of memory'.
        """
        geometry_cols = geometry_cols or []
        if pandas_df.empty:
            logger.info("DataFrame is empty, skipping upload.")
            return

        pool = self.get_pool()
        if not pool:
            raise RuntimeError(
                "Connection pool is not available; cannot run parallel upload.")

        # 1. Prepare Data & SQL
        if geometry_cols:
            pandas_df[geometry_cols] = pandas_df[geometry_cols].astype(str)

        values_string_list = [
            f"SDO_GEOMETRY(:{i}, {srid})" if col in geometry_cols else f":{i}"
            for i, col in enumerate(pandas_df.columns, start=1)
        ]
        sql_text = f"INSERT INTO {table_name} VALUES ({', '.join(values_string_list)})"

        # 2. Split DataFrame (The Fix)
        
        # New way (Safe): Split the range of row numbers, then slice the DF
        logger.info("Starting parallel upload. Threads: %d. Total Rows: %d",
                    num_threads, len(pandas_df))
        split_indices = np.array_split(np.arange(len(pandas_df)), num_threads)
        chunks = [pandas_df.iloc[indices] for indices in split_indices if len(indices) > 0]

        # 3. Worker Function
        def _worker_upload(chunk_df, chunk_id):
            """Insert one DataFrame chunk via a pooled connection, in sub-batches.

            :param chunk_df: The slice of the DataFrame this thread handles.
            :param chunk_id: Index of the chunk (used for error reporting).
            :return: Number of rows inserted by this thread.
            """
            if chunk_df.empty:
                return 0

            # Convert to list of tuples
            chunk_data = [tuple(row) for row in chunk_df.itertuples(index=False, name=None)]
            total_inserted_for_thread = 0

            try:
                # Acquire from pool
                with pool.acquire() as conn:
                    with conn.cursor() as cursor:
                        # Internal Loop: Send data in small batches to avoid memory overflow
                        for i in range(0, len(chunk_data), batch_size):
                            current_batch = chunk_data[i : i + batch_size]
                            cursor.executemany(sql_text, current_batch)
                            total_inserted_for_thread += cursor.rowcount
                    conn.commit()
                    return total_inserted_for_thread

            except oracledb.Error as e:
                logger.error("Thread %s error: %s", chunk_id, e)
                raise

        # 4. Execute Threads
        total_rows = 0
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_chunk = {
                executor.submit(_worker_upload, chunk, i): i
                for i, chunk in enumerate(chunks)
            }

            try:
                for future in as_completed(future_to_chunk):
                    total_rows += future.result()
            except Exception as e:
                logger.error("Upload failed: %s", e)
                raise

        logger.info('Done. Total rows inserted into "%s": %d', table_name, total_rows)

    def upload_pandas_df_to_oracle(self, pandas_df: pd.DataFrame, table_name: str,
                                   geometry_cols: list = None, srid: int = 4326) -> None:
        """
        Uploads a pandas DataFrame to an Oracle table using batched bulk insert.

        :param pandas_df: DataFrame to upload
        :param table_name: Target Oracle table name
        :param geometry_cols: List of geometry columns to handle with SDO_GEOMETRY
        :param srid: Spatial Reference ID for geometry columns
        """
        geometry_cols = geometry_cols or []
        values_string_list = [
            f"SDO_GEOMETRY(:{i}, {srid})" if col in geometry_cols else f":{i}"
            for i, col in enumerate(pandas_df.columns, start=1)
        ]
        values_string = ', '.join(values_string_list)

        if geometry_cols:
            pandas_df[geometry_cols] = pandas_df[geometry_cols].astype(str)

        try:
            pandas_tuples = [tuple(row) for row in pandas_df.itertuples(
                index=False, name=None)]
            sql_text = f"INSERT INTO {table_name} VALUES ({values_string})"

            with oracledb.connect(user=self.user, password=self.password, dsn=self.dsn) as oracle_conn:
                with oracle_conn.cursor() as oracle_cursor:
                    row_count = 0
                    batch_size = 15000
                    for i in range(0, len(pandas_tuples), batch_size):
                        batch = pandas_tuples[i:i + batch_size]
                        oracle_cursor.executemany(sql_text, batch)
                        row_count += oracle_cursor.rowcount
                        logger.info(
                            "Inserted batch %d, total rows inserted: %d",
                            i // batch_size + 1, row_count)

                oracle_conn.commit()
                logger.info('Number of new added rows in "%s": %d', table_name, row_count)

        except oracledb.DatabaseError as e:
            logger.error('Error during insertion: %s', e)
            raise

    def upsert_from_pandas_df(self, pandas_df: pd.DataFrame, table_name: str,
                          list_of_keys: list, clob_columns: list = None,
                          sum_update_columns: list = None) -> None:
        """
        Performs a upsert (merge) from a pandas DataFrame to an Oracle table,
        with reliable handling for CLOB data types.

        :param pandas_df: DataFrame containing data to upsert
        :param table_name: Target Oracle table name
        :param list_of_keys: List of columns to be used as keys for matching
        :param clob_columns: List of columns that are of the CLOB data type
        :param sum_update_columns: List of columns where updates should sum existing values with new ones
        """
        clob_columns = clob_columns or []
        sum_update_columns = sum_update_columns or []
        list_of_all_columns = pandas_df.columns.tolist()
        list_regular_columns = [
            col for col in list_of_all_columns if col not in list_of_keys]

        # No changes to SQL generation
        column_selection = ',\n'.join(
            [f":{col} AS {col}" for col in list_of_all_columns])

        matched_selection = ',\n'.join([
            f"t.{col} = t.{col} + s.{col}" if col in sum_update_columns else f"t.{col} = s.{col}"
            for col in list_regular_columns
        ])

        merge_sql = f"""
        MERGE INTO {table_name} t
        USING (
            SELECT
                {column_selection}
            FROM dual
        ) s
        ON ({' AND '.join([f"t.{key} = s.{key}" for key in list_of_keys])})
        WHEN MATCHED THEN
            UPDATE SET
                {matched_selection}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(list_of_all_columns)})
            VALUES ({', '.join([f"s.{col}" for col in list_of_all_columns])})
        """

        data_list = pandas_df.to_dict(orient='records')

        try:
            with oracledb.connect(user=self.user, password=self.password, dsn=self.dsn) as oracle_conn:
                with oracle_conn.cursor() as oracle_cursor:
                    
                    # --- START OF ADDED CODE ---
                    # Create a type map for setinputsizes. Default to None.
                    type_map = {col: oracledb.DB_TYPE_CLOB for col in clob_columns}
                    
                    # Set the input types for all columns, specifying CLOB where needed.
                    # This must be done BEFORE executemany.
                    oracle_cursor.setinputsizes(**type_map)
                    # --- END OF ADDED CODE ---

                    row_count = 0
                    batch_size = 15000  # Note: smaller batch sizes may be needed for very large CLOBs
                    for i in range(0, len(data_list), batch_size):
                        batch = data_list[i:i + batch_size]
                        oracle_cursor.executemany(merge_sql, batch)
                        row_count += oracle_cursor.rowcount
                        logger.info(
                            "Processed batch %d, total rows processed: %d",
                            i // batch_size + 1, row_count)

                oracle_conn.commit()
                logger.info('Number of upserted rows in "%s": %d', table_name, row_count)

        except oracledb.DatabaseError as e:
            logger.error('Error during upsert: %s', e)
            raise


if __name__ == "__main__":
    pass
