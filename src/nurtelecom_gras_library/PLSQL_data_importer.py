import cx_Oracle
import pandas as pd
import timeit
from sqlalchemy.engine import create_engine
from sqlalchemy import update
from sqlalchemy import text
from nurtelecom_gras_library.additional_functions import measure_time

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
    def export_to_file(self, query, path, is_csv=True, sep=';'):
        """
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
                        partial_df.to_csv(f, index=False, header=(i == 0), sep=sep)
                    else:
                        if i == 0:
                            partial_df.to_json(f, orient='records', lines=True)
                        else:
                            partial_df.to_json(f, orient='records', lines=True, header=False)

        except Exception as e:
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

    def upload_pandas_df_to_oracle_experimental(self, pandas_df, table_name, geometry_cols=[], batch_size=15000):
        # Prepare the values string for SQL statement
        values_string_list = [
            f":{i}" if col not in geometry_cols else f"SDO_UTIL.FROM_WKTGEOMETRY(:{i})"
            for i, col in enumerate(pandas_df.columns, start=1)
        ]
        values_string = ', '.join(values_string_list)

        # Convert geometry columns to string if necessary
        if geometry_cols:
            for geo_col in geometry_cols:
                pandas_df[geo_col] = pandas_df[geo_col].astype(str)

        # Prepare the data for insertion
        pandas_tuple = [tuple(i) for i in pandas_df.to_numpy()]
        sql_text = f"insert into {table_name} values({values_string})"

        try:
            # Use existing engine and connection management
            self.engine = self.get_engine()
            with self.engine.connect() as conn:
                rowCount = 0
                for start_pos in range(0, len(pandas_tuple), batch_size):
                    data_batch = pandas_tuple[start_pos:start_pos + batch_size]
                    conn.executemany(sql_text, data_batch)
                    rowCount += len(data_batch)

                # Update SDO_SRID for geometry columns if necessary
                if geometry_cols:
                    for geo_col in geometry_cols:
                        update_sdo_srid = f"UPDATE {table_name} SET {geo_col}.SDO_SRID = 4326 WHERE {geo_col} IS NOT NULL"
                        conn.execute(update_sdo_srid)
                        print(f'SDO_SRID of "{geo_col}" is updated to "4326"')

                print(
                    f'Number of new added rows in "{table_name}": {rowCount}')
                self.engine.dispose()
        except Exception as e:
            print(f'Error during insertion: {e}')
            raise

    def upload_pandas_df_to_oracle(self, pandas_df, table_name, geometry_cols=[]):
        values_string_list = [
            f":{i}" if v not in geometry_cols else f"SDO_UTIL.FROM_WKTGEOMETRY(:{i})" for i, v in enumerate(pandas_df, start=1)]
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
            self.dsn_tns = cx_Oracle.makedsn(
                self.host,
                self.port,
                service_name=self.service_name)

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
                if len(geometry_cols) != 0:
                    for geo_col in geometry_cols:
                        update_sdo_srid = f'''UPDATE {table_name} T
                                    SET T.{geo_col}.SDO_SRID = 4326
                                    WHERE T.{geo_col} IS NOT NULL'''
                        oracle_cursor.execute(update_sdo_srid)
                        print(f'SDO_SRID of "{geo_col}" is updated to "4326" ')
                    oracle_conn.commit()

        except:
            print('Error during insertion')
            if oracle_conn:

                oracle_conn.close()
                print('oracle connection is closed!')
            raise Exception

    def upsert_from_pandas_df_experimental(self, pandas_df, table_name, list_of_keys, sum_update_columns=[], batch_size=15000):
        # Column Lists
        list_of_all_columns = list(pandas_df.columns)
        list_regular_columns = list(
            set(list_of_all_columns) - set(list_of_keys))

        # SQL Query Building
        column_selection = ',\n'.join(
            [f'\t:{col} AS {col}' for col in list_of_all_columns])
        key_conditions = ' AND '.join(
            [f"t.{key} = s.{key}" for key in list_of_keys])
        update_selection = ',\n'.join(
            [f"t.{col} = {'t.' + col + ' + ' if col in sum_update_columns else ''}s.{col}" for col in list_regular_columns])

        merge_sql = f"""
        MERGE INTO {table_name} t
        USING (SELECT {column_selection} FROM dual) s
        ON ({key_conditions})
        WHEN MATCHED THEN UPDATE SET {update_selection}
        WHEN NOT MATCHED THEN INSERT ({', '.join(list_of_all_columns)})
        VALUES ({', '.join([f"s.{col}" for col in list_of_all_columns])})
        """

        # Execute Query in Batches
        try:
            self.engine = self.get_engine()
            with self.engine.connect() as conn:
                data_list = pandas_df.to_dict(orient='records')
                rowCount = 0
                for start_pos in range(0, len(data_list), batch_size):
                    data_batch = data_list[start_pos:start_pos + batch_size]
                    conn.execute(text(merge_sql), *data_batch)
                    rowCount += len(data_batch)
                print(
                    f'Number of rows processed in "{table_name}": {rowCount}')
        except Exception as e:
            print(f'Error during upsert: {e}')
            raise
        finally:
            self.engine.dispose()

    def upsert_from_pandas_df(self, pandas_df, table_name, list_of_keys, sum_update_columns=[]):
        "connection"
        self.dsn_tns = cx_Oracle.makedsn(
            self.host,
            self.port,
            service_name=self.service_name)

        oracle_conn = cx_Oracle.connect(
            user=self.user,
            password=self.password,
            dsn=self.dsn_tns
        )
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
