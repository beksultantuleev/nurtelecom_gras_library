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

    def get_data(self, query,
                 remove_column=[],
                 remove_na=False,
                 show_logs=False):
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

    def execute(self, query):
        self.engine = create_engine(self.ENGINE_PATH_WIN_AUTH)
        self.conn = self.engine.connect()
        with self.engine.connect() as conn:
            conn.execute(text(query))  # text
            conn.close()
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

    'old deprecated'
    # def upload_pandas_df_to_oracle(self, pandas_df, table_name):
    #     try:
    #         values_string = self.value_creator(pandas_df.shape[1])
    #         pandas_tuple = [tuple(i) for i in pandas_df.values]
    #         sql_text = f'insert into {table_name} values({values_string})'

    #         self.dsn_tns = cx_Oracle.makedsn(
    #             self.host,
    #             self.port,
    #             service_name=self.service_name)

    #         oracle_conn = cx_Oracle.connect(
    #             user=self.user,
    #             password=self.password,
    #             dsn=self.dsn_tns
    #         )
    #         # oracle_cursor = oracle_conn.cursor()
    #         with oracle_conn.cursor() as oracle_cursor:
    #             ####
    #             rowCount = 0
    #             start_pos = 0
    #             batch_size = 15000
    #             while start_pos < len(pandas_tuple):
    #                 data_ = pandas_tuple[start_pos:start_pos + batch_size]
    #                 start_pos += batch_size
    #                 oracle_cursor.executemany(sql_text, data_)
    #                 rowCount += oracle_cursor.rowcount
    #             ###
    #             print(f'number of new added rows >>{rowCount}')
    #             oracle_conn.commit()
    #     except:
    #         print('Error during insertion')
    #         if oracle_conn:

    #             oracle_conn.close()
    #             print('oracle connection is closed!')
    #         raise Exception

    'do not need anymore'
    # def value_creator(self, num_of_columns):
    #     'this function is used for upload pandas to oracle'
    #     string_values = ''
    #     for i in range(1, num_of_columns+1):
    #         string_values += f':{i}, ' if i != num_of_columns else f':{i}'
    #     return string_values


if __name__ == "__main__":
    pass
