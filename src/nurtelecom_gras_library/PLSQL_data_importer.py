import cx_Oracle
import pandas as pd
import timeit
from sqlalchemy.engine import create_engine
from sqlalchemy import update, text
from nurtelecom_gras_library.additional_functions import measure_time
import csv
import json

class PLSQL_data_importer():

    def __init__(self, user,
                 password,
                 host,
                 port='1521',
                 service_name='DWH') -> None:

        self.host = host
        self.port = port
        self.service_name = service_name
        self.user = user
        self.password = password

        self.dsn_tns = cx_Oracle.makedsn(
            self.host,
            self.port,
            service_name=self.service_name)

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
                self._engine = create_engine(self.ENGINE_PATH_WIN_AUTH)
            except Exception as e:
                print(f"Error creating engine: {e}")
                raise
        return self._engine

    @measure_time
    def get_data(self, query, remove_column=None, remove_na=False, show_logs=False):
        """
        Retrieve data from the database based on a SQL query.

        :param query: SQL query for data retrieval
        :param remove_column: Columns to remove from the resulting DataFrame, defaults to None
        :param remove_na: Flag to indicate if NA values should be dropped, defaults to False
        :param show_logs: Flag to indicate if logs should be shown, defaults to False
        """
        remove_column = remove_column or []
        try:
            query = text(query)
            engine = create_engine(self.ENGINE_PATH_WIN_AUTH)

            start = timeit.default_timer()
            with engine.connect() as conn:
                data = pd.read_sql(query, con=conn)
                data.columns = data.columns.str.lower()
                data.drop(remove_column, axis=1, inplace=True)
                if remove_na:
                    data.dropna(inplace=True)

            if show_logs:
                print(data.head(5))
            return data

        except Exception as e:
            print(f"Error during data retrieval: {e}")
            raise

    def get_data_old(self, query,
                     remove_column=[],
                     remove_na=False,
                     show_logs=False):
        query = text(query)
        'establish connection and return data'
        start = timeit.default_timer()

        self.engine = create_engine(self.ENGINE_PATH_WIN_AUTH)
        self.conn = self.engine.connect()
        data = pd.read_sql(query, con=self.conn)
        data.columns = data.columns.str.lower()
        data = data.drop(remove_column, axis=1)
        if remove_na:
            data = data.dropna()
        stop = timeit.default_timer()
        if show_logs:
            print(data.head(5))
            print(f"end, time is {(stop - start) / 60:.2f} min")
        self.conn.close()
        self.engine.dispose()
        return data

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
                            f, index=False, header=(i == 0), sep=sep, encoding = encoding)
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
    def export_to_file_oracle(self, query, path, is_csv=True, sep=',', encoding='utf-8', chunk_size=1000):
        """
        utf-8-sig for Cyrillic  
        Export data from an Oracle database query to a file using cx_Oracle and csv module, with progress tracking.

        :param query: SQL query to export data
        :param path: File path to export the data
        :param is_csv: Boolean flag to determine if the output should be CSV (default) or JSON
        :param sep: Separator for CSV file, defaults to ','
        :param encoding: Encoding format to be used for writing the file, defaults to 'utf-8'
        :param chunk_size: Number of rows to process at a time, default is 1000
        """
        try:
            # Establish Oracle connection
            connection = cx_Oracle.connect(
                user=self.user,
                password=self.password,
                dsn=self.dsn_tns
            )
            cursor = connection.cursor()

            # Execute the query
            cursor.execute(query)

            # Open file for writing
            with open(path, 'w', newline='', encoding=encoding) as f:
                writer = None
                if is_csv:
                    writer = csv.writer(f, delimiter=sep)

                # Write headers
                if is_csv:
                    column_names = [col[0] for col in cursor.description]
                    writer.writerow(column_names)

                # Initialize row counter
                row_count = 0
                chunk_count = 0

                # Write rows in chunks to track progress
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break  # No more rows to fetch

                    chunk_count += 1
                    row_count += len(rows)

                    if is_csv:
                        writer.writerows(rows)
                    else:
                        for row in rows:
                            json_data = {column_names[i]: value for i, value in enumerate(row)}
                            f.write(json.dumps(json_data) + '\n')

                    print(f"Chunk {chunk_count} written, {len(rows)} rows in this chunk, {row_count} total rows written.")

            cursor.close()
            connection.close()

            print(f"Export complete. {row_count} rows written in {chunk_count} chunks.")

        except cx_Oracle.DatabaseError as e:
            print(f"Error during export: {e}")
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
            engine = create_engine(self.ENGINE_PATH_WIN_AUTH)
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

    def execute_old(self, query, verbose=False):
        query = text(query)
        self.engine = create_engine(self.ENGINE_PATH_WIN_AUTH)
        self.conn = self.engine.connect()
        with self.engine.connect() as conn:
            conn.execute(query)  # text
            conn.close()
            if verbose:
                print('Connection in execute is closed!')
        self.conn.close()
        self.engine.dispose()

    def upload_pandas_df_to_oracle(self, pandas_df, table_name, geometry_cols=[], srid = 4326):
        values_string_list = [
            f":{i}" if v not in geometry_cols else f"SDO_GEOMETRY(:{i}, {srid})" for i, v in enumerate(pandas_df.columns, start=1)
        ]
        values_string = ', '.join(values_string_list)
        if len(geometry_cols) != 0:
            for geo_col in geometry_cols:
                pandas_df.loc[:, geo_col] = pandas_df.loc[:,
                                                          geo_col].astype(str)
        try:
            # values_string = value_creator(pandas_df.shape[1])
            pandas_tuple = [tuple(i) for i in pandas_df.values]
            sql_text = f"insert into {table_name} values({values_string})"
            # print(sql_text)

            oracle_conn = cx_Oracle.connect(
                user=self.user,
                password=self.password,
                dsn=self.dsn_tns
            )
            # oracle_cursor = oracle_conn.cursor()
            with oracle_conn.cursor() as oracle_cursor:
                ####
                rowCount = 0
                start_pos = 0
                batch_size = 15000
                while start_pos < len(pandas_tuple):
                    data_ = pandas_tuple[start_pos:start_pos + batch_size]
                    start_pos += batch_size
                    oracle_cursor.executemany(sql_text, data_)
                    rowCount += oracle_cursor.rowcount
                ###
                print(
                    f'number of new added rows in "{table_name}" >>{rowCount}')
                oracle_conn.commit()
                'do not need further part anymore'
                # if len(geometry_cols) != 0:
                #     for geo_col in geometry_cols:
                #         update_sdo_srid = f'''UPDATE {table_name} T
                #                     SET T.{geo_col}.SDO_SRID = {srid}
                #                     WHERE T.{geo_col} IS NOT NULL'''
                #         oracle_cursor.execute(update_sdo_srid)
                #         print(f'SDO_SRID of "{geo_col}" is updated to "{srid}" ')
                #     oracle_conn.commit()

        except:
            print('Error during insertion')
            if oracle_conn:

                oracle_conn.close()
                print('oracle connection is closed!')
            raise Exception

    def upload_pandas_df_to_oracle_row(self, pandas_df, table_name, geometry_cols=[], srid=4326):
        'uploads row by row using clob'
        # Prepare values string for the SQL insert statement
        columns = pandas_df.columns
        values_string = ', '.join([
            f"SDO_GEOMETRY(:{i+1}, {srid})" if col in geometry_cols else f":{i+1}"
            for i, col in enumerate(columns)
        ])
        sql_text = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values_string})"

        # Convert geometry columns to WKT
        for geo_col in geometry_cols:
            pandas_df[geo_col] = pandas_df[geo_col].apply(
                lambda geom: geom.wkt if geom else None
            )

        try:
            # Establish Oracle connection
            oracle_conn = cx_Oracle.connect(
                user=self.user,
                password=self.password,
                dsn=self.dsn_tns
            )

            with oracle_conn.cursor() as oracle_cursor:
                rowCount = 0

                for index, row in pandas_df.iterrows():
                    bind_row = []
                    input_sizes = []

                    for col, value in zip(columns, row):
                        if col in geometry_cols and value is not None:
                            clob_var = oracle_cursor.var(cx_Oracle.CLOB)
                            clob_var.setvalue(0, value)
                            bind_row.append(clob_var)
                            input_sizes.append(cx_Oracle.CLOB)
                        else:
                            bind_row.append(value)
                            input_sizes.append(None)

                    try:
                        # Set input sizes for the row
                        oracle_cursor.setinputsizes(*input_sizes)
                        oracle_cursor.execute(sql_text, tuple(bind_row))
                        rowCount += 1
                        oracle_conn.commit()
                        print(f'Number of added rows so far: {rowCount}')
                    except cx_Oracle.DatabaseError as e:
                        error, = e.args
                        print(
                            f"Error at row {index}: Oracle-Error-Code: {error.code}, Oracle-Error-Message: {error.message}")
                        continue
                'no need anymore'
                # if len(geometry_cols) != 0:
                #     for geo_col in geometry_cols:
                #         update_sdo_srid = f'''
                #             UPDATE {table_name} T
                #             SET T.{geo_col}.SDO_SRID = {srid}
                #             WHERE T.{geo_col} IS NOT NULL
                #         '''
                #         oracle_cursor.execute(update_sdo_srid)
                #         print(f'SDO_SRID of "{geo_col}" is updated to "{srid}"')
                #     oracle_conn.commit()

                print(f'Number of new added rows in "{table_name}": {rowCount}')

        except Exception as e:
            print('Error during insertion')
            print(str(e))
            raise

        finally:
            if oracle_conn:
                oracle_conn.close()
                print('Oracle connection is closed!')

    def upsert_from_pandas_df(self, pandas_df, table_name, list_of_keys, sum_update_columns=[]):
        "connection"

        # dsn_tns = cx_Oracle.makedsn(host, port, service)
        # oracle_conn = cx_Oracle.connect(user=user, password=passwd, dsn=dsn_tns)
        "create query "
        list_of_all_columns = pandas_df.columns
        list_regular_columns = list(
            set(list_of_all_columns) - set(list_of_keys))

        column_selection = ''
        for col in list_of_all_columns:
            column_selection += f'\t:{col} AS {col},\n'

        list_of_processed_keys = []
        for key in list_of_keys:
            key_selection = ''
            key_selection += f"t.{key} = s.{key}"
            list_of_processed_keys.append(key_selection)

        # print(list_of_processed_keys)
        matched_selection = ''
        for col in list_regular_columns:
            if col not in sum_update_columns:
                matched_selection += f"t.{col} = s.{col},\n"
            else:
                matched_selection += f"t.{col} = t.{col} + s.{col},\n"

        # print(matched_selection)

        merge_sql = f"""
        MERGE INTO {table_name} t
                USING (
                SELECT
        {column_selection[:-2]}
                FROM dual
                        ) s
            ON ({" AND ".join(list_of_processed_keys)})
                WHEN MATCHED THEN
                UPDATE SET {matched_selection[:-2]}
                WHEN NOT MATCHED THEN
                    INSERT ({", ".join(list_of_all_columns)})
                    VALUES ({", ".join([f"s.{i}" for i in list_of_all_columns])})
        """
        # print(merge_sql)
        data_list = pandas_df.to_dict(orient='records')
        # cursor.executemany(merge_sql, data_list)
        ####
        try:
            oracle_conn = cx_Oracle.connect(
                user=self.user,
                password=self.password,
                dsn=self.dsn_tns
            )
            with oracle_conn.cursor() as oracle_cursor:
                rowCount = 0
                start_pos = 0
                batch_size = 15000
                while start_pos < len(data_list):
                    data_ = data_list[start_pos:start_pos + batch_size]
                    start_pos += batch_size
                    oracle_cursor.executemany(merge_sql, data_)
                    rowCount += oracle_cursor.rowcount
                ###
                print(
                    f'number of new added rows in "{table_name}" >>{rowCount}')

            # Commit the changes
            oracle_conn.commit()
            # Close the connection
            oracle_conn.close()
        except:
            print('Error during upsert!')
            if oracle_conn:
                oracle_conn.close()
                print('oracle connection is closed!')
            raise Exception


if __name__ == "__main__":
    pass
