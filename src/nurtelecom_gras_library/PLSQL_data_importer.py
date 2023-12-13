import cx_Oracle
import pandas as pd
import timeit
from sqlalchemy.engine import create_engine
from sqlalchemy import update
from sqlalchemy import text


'this is light version (no geopandas)'


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
        '''use engine for making manual connection 
        for instance: 
        
        engine = database_connector.get_engine()
        conn = engine.connect()

        Do not forget to close connection 
        
        conn.close()
        '''
        engine = create_engine(self.ENGINE_PATH_WIN_AUTH)
        return engine

    def get_data(self, query,
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

    def export_to_file(self, query, path, is_csv=True, sep=';'):
        query = text(query)
        'file_extension could be csv or JSON'
        self.engine = create_engine(self.ENGINE_PATH_WIN_AUTH)
        self.conn = self.engine.connect()
        start = timeit.default_timer()
        with open(path, 'w') as f:
            for i, partial_df in enumerate(pd.read_sql(query, self.conn, chunksize=100000)):
                print(f'Writing chunk "{i}" of the table >> "{path}"')
                if is_csv:
                    partial_df.to_csv(f, index=False, header=(i == 0), sep=sep)
                else:
                    partial_df.to_json(f)
                # else:
                #     print("cannot do this format!")
        stop = timeit.default_timer()
        self.conn.close()
        self.engine.dispose()
        print(f"end, time is {(stop - start) / 60:.2f} min")

    def truncate_table_with_warning(self, table_name):
        '''Be careful with this function'''
        yes_answers = ['yes', 'y', 'yep', 'hell yea']
        user_answer = input(
            f"Do u really want to truncate table '{table_name}'?? Type 'y' or 'n' to continue...\n")
        if user_answer.lower() in yes_answers:
            trunc_query = (f'''
            TRUNCATE TABLE {table_name}
            ''')
            self.execute(query=trunc_query)
            print(f"Table {table_name} truncated!")
        else:
            print('Truncation is aborted!')

    def truncate_table(self, table_name):
        '''Be careful with this function'''
        trunc_query = (f'''
        TRUNCATE TABLE {table_name}
        ''')
        self.execute(query=trunc_query)
        print(f"Table {table_name} truncated!")

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

    def execute(self, query, verbose = False):
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

    def upsert_from_pandas_df(self, pandas_df, table_name, list_of_keys, sum_update_columns = [] ):
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
        list_regular_columns = list(set(list_of_all_columns)- set(list_of_keys))

        column_selection = ''
        for col in list_of_all_columns:
            column_selection+=f'\t:{col} AS {col},\n'

        list_of_processed_keys = []
        for key in list_of_keys:
            key_selection = ''
            key_selection+=f"t.{key} = s.{key}"
            list_of_processed_keys.append(key_selection)

        # print(list_of_processed_keys)
        matched_selection = ''
        for col in list_regular_columns:
            if col not in sum_update_columns:
                matched_selection+=f"t.{col} = s.{col},\n"
            else:
                matched_selection+=f"t.{col} = t.{col} + s.{col},\n"

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
