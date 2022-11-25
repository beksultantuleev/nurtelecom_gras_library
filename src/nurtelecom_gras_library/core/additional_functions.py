from genericpath import isdir
import os
import shutil


def make_clob_query_from_pandas(data, counter, list_of_column_names, table_name, full_data_length=None):
    'very rare useage'
    num_of_features = len(list_of_column_names)
    payload = ''
    int_float_list = ['int64', 'int32', 'float32', 'float64', 'float']
    query_declare = f'''\n
    DECLARE
    '''
    query_begin_end = f'''
    BEGIN\n'''

    for index, column_type in enumerate(list_of_column_names):
        if data[column_type].dtype == 'object':
            payload += f'clob_{index}, ' if num_of_features - \
                1 != index else f'clob_{index}'
            query_declare += f'clob_{index} clob;\n'
            query_begin_end += f'''clob_{index} := {merge_clob_maker(data[column_type][counter])};\n'''
        elif data[column_type].dtype in int_float_list:
            payload += f'numeric_{index}, ' if num_of_features - \
                1 != index else f'numeric_{index}'
            query_declare += f'numeric_{index} numeric;\n'
            query_begin_end += f'''numeric_{index} := '{data[column_type][counter]}';\n'''
    query_insert = f'''
    \nINSERT INTO {table_name}
                    VALUES({payload});
    COMMIT;
    END;
    '''
    if full_data_length != None:
        print(f'insertion at {counter/full_data_length*100:.2f}% complete')
    return query_declare + query_begin_end + query_insert


def insert_from_pandas(data, counter, list_of_column_names, full_data_length=None):
    '''data is dataframe
    counter - counter of a row
    list_of_column_names - columns to incert into table'''
    modified_query = "".join(
        "," + f"'{data[i][counter]}'" for i in list_of_column_names)[1:]
    if full_data_length != None:
        print(f'Inserting... ({counter/full_data_length*100:.2f}%)')
    return modified_query


def get_list_of_objects(path, is_dir=False):
    'to get list of files in certain directory'
    list_of_file_names = []
    for file in os.listdir(path):
        if is_dir:
            if os.path.isdir(os.path.join(path, file)):
                list_of_file_names.append(file)
        else:
            if os.path.isfile(os.path.join(path, file)):
                list_of_file_names.append(file)
    return list_of_file_names


def merge_clob_maker(string_to_split, num_of_charr=25000):
    'to be able to fit any query from python to sql'
    req = ''
    list_of_query = [string_to_split[i:i+num_of_charr]
                     for i in range(0, len(string_to_split), num_of_charr)]
    count = 0
    for c in list_of_query:
        if count == len(list_of_query)-1:
            req += f'''to_clob('{c}')'''
        else:
            req += f'''to_clob('{c}') || '''
        count += 1
    return req


def value_extractor(pattern, path):
    with open(path) as f:
        lines = f.read().splitlines()
        for i in lines:
            if pattern in i:
                value = float(i[len(pattern):])
                return value


def make_table_query_from_pandas(df, table_name, varchar_len=1000, list_num_columns=[], list_date_columns=[], list_clob_columns=[]):
    query_for_creating_table = f'CREATE TABLE {table_name} (\n'
    for column in df:
        if column in list_num_columns:
            query_for_creating_table += f"""{column} \t number,\n"""
        elif column in list_date_columns:
            query_for_creating_table += f"""{column} \t date,\n"""
        elif column in list_clob_columns:
            query_for_creating_table += f"""{column} \t clob,\n"""
        else:
            query_for_creating_table += f"""{column} \t varchar2({varchar_len}),\n"""
    query_for_creating_table = query_for_creating_table[:-2]
    query_for_creating_table += '\n)'
    return query_for_creating_table


def send_telegram_msg(payload, receiver, database_connector):
    'updated send_telegram logic'
    payload = payload.replace("'", '')
    if type(receiver) is str:
        query_for_msg = f'''
        BEGIN 
            kpi_bot.tb_message_insert('{receiver}', '{payload}'); 
        END;
        '''
        database_connector.execute(query_for_msg)
    else:
        for rec in receiver:
            query_for_msg = f'''
            BEGIN 
                kpi_bot.tb_message_insert('{rec}', '{payload}'); 
            END;
            '''
            database_connector.execute(query_for_msg)


def send_sms(payload, receiver, database_connector):
    payload = payload.replace("'", '')
    query_for_msg = f'''
    BEGIN 
        kpi.kpi_sms_to_send(msisdn => '{receiver}', sms_txt => '{payload}');
        COMMIT;
    END;
    '''
    database_connector.execute(query_for_msg)


def get_a_copy(path_to_original_file, end_path):
    try:
        shutil.copyfile(path_to_original_file,
                        end_path)
        print('copy complete!')
    except:
        print('failed')


def error_sender(exception_error, project_name, list_of_phone_numbers, database_connector):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    full_error_msg = f"""Warning!\nError occurred in "{project_name}" at <code>{dir_path}</code>\n
    """ + str(exception_error).replace("'", '"')
    send_telegram_msg(payload=full_error_msg, receiver=list_of_phone_numbers,
                      database_connector=database_connector)


if __name__ == "__main__":
    pass