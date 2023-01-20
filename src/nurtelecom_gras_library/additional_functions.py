from genericpath import isdir
import os
import shutil
import smtplib
import mimetypes
from argparse import ArgumentParser
from email.message import EmailMessage
from email.policy import SMTP


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


def make_table_query_from_pandas(df, table_name, varchar_len=500, list_num_columns=[], list_date_columns=[], list_geometry_columns = [], list_clob_columns=[]):
    query_for_creating_table = f'CREATE TABLE {table_name} (\n'
    for column in df:
        if column in list_num_columns:
            query_for_creating_table += f"""{column} \t number,\n"""
        elif column in list_date_columns:
            query_for_creating_table += f"""{column} \t date,\n"""
        
        elif column in list_clob_columns:
            query_for_creating_table += f"""{column} \t clob,\n"""

        elif column in list_geometry_columns:
            query_for_creating_table += f"""{column} \t sdo_geometry,\n"""
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
    if type(receiver) is list:
        for receiv in receiver:
            query_for_msg = f'''
            BEGIN 
                kpi.kpi_sms_to_send(msisdn => '{receiv}', sms_txt => '{payload}');
                COMMIT;
            END;
            '''
            database_connector.execute(query_for_msg)
    else:
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


def send_email(send_to, send_from, subject, host, content=None, directory=None, file_to_attach=None, show_logs = False):

    # Create the message
    msg = EmailMessage()
    # f'Contents of directory {os.path.abspath(directory)}'
    msg['Subject'] = subject
    msg['To'] = send_to
    msg['From'] = send_from
    msg.preamble = 'You will not see this in a MIME-aware mail reader.\n'
    if content != None:
        msg.set_content(f"""{content}
        """)
    if file_to_attach != None:
        filename = file_to_attach.split(
            '\\') if '\\' in file_to_attach else file_to_attach.split('/')
        # print(filename)
        ctype, encoding = mimetypes.guess_type(file_to_attach)
        if ctype is None or encoding is not None:
            # No guess could be made, or the file is encoded (compressed), so
            # use a generic bag-of-bits type.
            ctype = 'application/octet-stream'
        maintype, subtype = ctype.split('/', 1)
        with open(file_to_attach, 'rb') as fp:
            msg.add_attachment(fp.read(),
                               maintype=maintype,
                               subtype=subtype,
                               filename=filename[-1]
                               )

    if directory != None:
        for filename in os.listdir(directory):
            path = os.path.join(directory, filename)
            if not os.path.isfile(path):
                continue
            # Guess the content type based on the file's extension.  Encoding
            # will be ignored, although we should check for simple things like
            # gzip'd or compressed files.
            ctype, encoding = mimetypes.guess_type(path)
            if ctype is None or encoding is not None:
                # No guess could be made, or the file is encoded (compressed), so
                # use a generic bag-of-bits type.
                ctype = 'application/octet-stream'
            maintype, subtype = ctype.split('/', 1)
            with open(path, 'rb') as fp:
                msg.add_attachment(fp.read(),
                                   maintype=maintype,
                                   subtype=subtype,
                                   filename=filename)
    # Now send or store the message

    with smtplib.SMTP(host, port=25) as s:
        s.send_message(msg)
    if show_logs:
        print(f'email sent from >> {send_from} to >> {send_to}')


def error_sender(exception_error, dir_path, project_name, list_of_phone_numbers, database_connector):
    # dir_path = os.path.dirname(os.path.realpath(__file__))
    full_error_msg = f"""Warning!\nError occurred in "{project_name}" at <code>{dir_path}</code>\n
    """ + str(exception_error)[:3500].replace("'", '"')
    send_telegram_msg(payload=full_error_msg, receiver=list_of_phone_numbers,
                      database_connector=database_connector)




'do not need'
# def make_clob_query_from_pandas(data, counter, list_of_column_names, table_name, full_data_length=None):
#     'very rare useage'
#     num_of_features = len(list_of_column_names)
#     payload = ''
#     int_float_list = ['int64', 'int32', 'float32', 'float64', 'float']
#     query_declare = f'''\n
#     DECLARE
#     '''
#     query_begin_end = f'''
#     BEGIN\n'''

#     for index, column_type in enumerate(list_of_column_names):
#         if data[column_type].dtype == 'object':
#             payload += f'clob_{index}, ' if num_of_features - \
#                 1 != index else f'clob_{index}'
#             query_declare += f'clob_{index} clob;\n'
#             query_begin_end += f'''clob_{index} := {merge_clob_maker(data[column_type][counter])};\n'''
#         elif data[column_type].dtype in int_float_list:
#             payload += f'numeric_{index}, ' if num_of_features - \
#                 1 != index else f'numeric_{index}'
#             query_declare += f'numeric_{index} numeric;\n'
#             query_begin_end += f'''numeric_{index} := '{data[column_type][counter]}';\n'''
#     query_insert = f'''
#     \nINSERT INTO {table_name}
#                     VALUES({payload});
#     COMMIT;
#     END;
#     '''
#     if full_data_length != None:
#         print(f'insertion at {counter/full_data_length*100:.2f}% complete')
#     return query_declare + query_begin_end + query_insert


if __name__ == "__main__":
    pass
