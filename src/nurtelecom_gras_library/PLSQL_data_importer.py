# import cx_Oracle
import oracledb
import pandas as pd
import timeit
from sqlalchemy.engine import create_engine
from sqlalchemy import update, text
from nurtelecom_gras_library.additional_functions import measure_time
import csv
import json


class PLSQL_data_importer():
    oracledb.init_oracle_client()

    def __init__(self, user: str, password: str, host: str,
                 port: str = '1521', service_name: str = 'DWH') -> None:
        self.host = host
        self.port = port
        self.service_name = service_name
        self.user = user
        self.password = password
        self.dsn = oracledb.makedsn(self.host, self.port, service_name=self.service_name)
        self.engine_url = f'oracle+oracledb://{self.user}:{self.password}@{self.dsn}'
        
        self.ENGINE_PATH_WIN_AUTH = f'oracle://{self.user}:{self.password}@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={self.host})(PORT={self.port}))(CONNECT_DATA=(SERVICE_NAME={self.service_name})))'

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
        if not hasattr(self, '_engine'):
            try:
                # self._engine = create_engine(self.ENGINE_PATH_WIN_AUTH)
                self._engine = create_engine(
                    self.engine_url, echo=False, future=True)
            except Exception as e:
                print(f"Error creating engine: {e}")
                raise
        return self._engine

    @measure_time
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
                print(data.head(5))
            return data

        except Exception as e:
            print(f"Error during data retrieval: {e}")
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
            engine = create_engine(self.ENGINE_PATH_WIN_AUTH)

            with engine.connect() as conn, open(path, 'w') as f:
                for i, partial_df in enumerate(pd.read_sql(query, conn, chunksize=100000)):
                    print(f'Writing chunk "{i}" to "{path}"')
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
            print(f"Error during export: {e}")
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

                        print(
                            f"Chunk {chunk_count} written, {len(rows)} rows in this chunk, {row_count} total rows written.")

            print(
                f"Export complete. {row_count} rows written in {chunk_count} chunks.")

        except oracledb.DatabaseError as e:
            error, = e.args
            print(f"Database error during export: {error.message}")
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

        try:
            trunc_query = f"TRUNCATE TABLE {table_name}"
            self.execute(query=trunc_query)
            print(f"Table '{table_name}' truncated successfully.")

        except Exception as e:
            print(f"Error occurred while truncating table '{table_name}': {e}")
            raise

    def final_query_for_insertion(self, table_name, payload=None, columns_to_insert=None):
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

    def execute(self, query, verbose=False):
        try:
            # Use text function for query safety
            query = text(query)

            # Create engine and execute query within context manager
            # engine = create_engine(self.ENGINE_PATH_WIN_AUTH)
            engine = self.get_engine()
            with engine.connect() as conn:
                conn.execute(query)
                if verbose:
                    print('Query executed successfully.')

        except Exception as e:
            if verbose:
                print(f'Error during query execution: {e}')
            raise  # Reraising the exception to be handled at a higher level if needed

        finally:
            # Dispose of the engine to close the connection properly
            engine.dispose()
            if verbose:
                print('Connection closed and engine disposed.')

    @measure_time
    def upload_pandas_df_to_oracle(self, pandas_df: pd.DataFrame, table_name: str,
                                   geometry_cols: list = [], srid: int = 4326) -> None:
        """
        Uploads a pandas DataFrame to an Oracle table using bulk insert.

        :param pandas_df: DataFrame to upload
        :param table_name: Target Oracle table name
        :param geometry_cols: List of geometry columns to handle with SDO_GEOMETRY
        :param srid: Spatial Reference ID for geometry columns
        """
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
                        print(
                            f"Inserted batch {i // batch_size + 1}, total rows inserted: {row_count}")

                oracle_conn.commit()
                print(
                    f'Number of new added rows in "{table_name}": {row_count}')

        except oracledb.DatabaseError as e:
            print('Error during insertion:', e)
            raise

    def upload_pandas_df_to_oracle_row(self, pandas_df: pd.DataFrame, table_name: str,
                                       geometry_cols: list = [], srid: int = 4326) -> None:
        """
        Uploads a pandas DataFrame to an Oracle table row by row, handling CLOBs for geometry columns.

        :param pandas_df: DataFrame to upload
        :param table_name: Target Oracle table name
        :param geometry_cols: List of geometry columns to handle with SDO_GEOMETRY
        :param srid: Spatial Reference ID for geometry columns
        """
        columns = pandas_df.columns
        values_string = ', '.join([
            f"SDO_GEOMETRY(:{i+1}, {srid})" if col in geometry_cols else f":{i+1}"
            for i, col in enumerate(columns)
        ])
        sql_text = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values_string})"

        for geo_col in geometry_cols:
            pandas_df[geo_col] = pandas_df[geo_col].apply(
                lambda geom: geom.wkt if geom else None)

        try:
            with oracledb.connect(user=self.user, password=self.password, dsn=self.dsn) as oracle_conn:
                with oracle_conn.cursor() as oracle_cursor:
                    row_count = 0

                    for index, row in pandas_df.iterrows():
                        bind_row = []
                        for i, (col, value) in enumerate(zip(columns, row)):
                            if col in geometry_cols and value is not None:
                                bind_row.append(value)
                            else:
                                bind_row.append(value)

                        try:
                            oracle_cursor.execute(sql_text, bind_row)
                            row_count += 1
                            oracle_conn.commit()
                            print(f'Number of added rows so far: {row_count}')
                        except oracledb.DatabaseError as e:
                            error, = e.args
                            print(
                                f"Error at row {index}: Oracle-Error-Code: {error.code}, Oracle-Error-Message: {error.message}")
                            continue

                print(
                    f'Number of new added rows in "{table_name}": {row_count}')

        except Exception as e:
            print('Error during insertion:', e)
            raise

    def upsert_from_pandas_df(self, pandas_df: pd.DataFrame, table_name: str,
                              list_of_keys: list, sum_update_columns: list = []) -> None:
        """
        Performs an upsert (merge) operation from a pandas DataFrame to an Oracle table.

        :param pandas_df: DataFrame containing data to upsert
        :param table_name: Target Oracle table name
        :param list_of_keys: List of columns to be used as keys for matching
        :param sum_update_columns: List of columns where updates should sum existing values with new ones
        """
        list_of_all_columns = pandas_df.columns.tolist()
        list_regular_columns = [
            col for col in list_of_all_columns if col not in list_of_keys]

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
                    row_count = 0
                    batch_size = 15000
                    for i in range(0, len(data_list), batch_size):
                        batch = data_list[i:i + batch_size]
                        oracle_cursor.executemany(merge_sql, batch)
                        row_count += oracle_cursor.rowcount
                        print(
                            f"Processed batch {i // batch_size + 1}, total rows processed: {row_count}")

                oracle_conn.commit()
                print(
                    f'Number of upserted rows in "{table_name}": {row_count}')

        except oracledb.DatabaseError as e:
            print('Error during upsert:', e)
            raise


if __name__ == "__main__":
    pass
