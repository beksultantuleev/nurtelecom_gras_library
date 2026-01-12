from genericpath import isdir
import os
import shutil
import smtplib
import mimetypes
from argparse import ArgumentParser
from email.message import EmailMessage
from email.policy import SMTP
import timeit
import requests
import base64
import hvac



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

def make_table_query_from_pandas_old(df, table_name, varchar_len=500, list_num_columns=[], list_date_columns=[], list_geometry_columns = [], list_clob_columns=[]):
    query_for_creating_table = f'CREATE TABLE {table_name} (\n'
    for column in df:
        column = f"{column}"[:25]
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

def make_table_query_from_pandas(
    df,
    table_name,
    varchar_len=500,
    list_num_columns=[],
    list_date_columns=[],
    list_timestamp_columns=[],
    list_geometry_columns=[],
    list_clob_columns=[],
    partition_column=None,
    partition_type=None,
    partition_granularity=None,     # 'MONTH', 'DAY', 'HOUR', etc.
    # e.g., "DATE '2024-01-01'" or "TIMESTAMP '2024-01-01 00:00:00'"
    partition_start=None,
    # e.g., "DATE '2025-01-01'" or "TIMESTAMP '2025-01-01 00:00:00'"
    partition_end=None,
    partition_interval=None,        # e.g., "INTERVAL '1' MONTH"
    partition_values=None           # for LIST/HASH
):
    """
    Generate a CREATE TABLE query from a pandas DataFrame with optional partitioning.

    Args:
        df (pd.DataFrame): DataFrame to generate table from.
        table_name (str): Name of the table.
        varchar_len (int): Length for varchar2 columns.
        list_num_columns (list): Columns to be NUMBER.
        list_date_columns (list): Columns to be DATE.
        list_timestamp_columns (list): Columns to be TIMESTAMP.
        list_geometry_columns (list): Columns to be SDO_GEOMETRY.
        list_clob_columns (list): Columns to be CLOB.
        partition_column (str, optional): Column to partition by.
        partition_type (str, optional): Type of partitioning ('RANGE', 'LIST', 'HASH', 'INTERVAL').
        partition_granularity (str, optional): For RANGE partitions, e.g., 'MONTH', 'DAY', 'HOUR'.
        partition_start (str, optional): Start value for RANGE/INTERVAL partitioning.
        partition_end (str, optional): End value for RANGE/INTERVAL partitioning.
        partition_interval (str, optional): Interval for INTERVAL partitioning, e.g., "INTERVAL '1' MONTH".
        partition_values (list, optional): Partition values (for LIST/HASH/RANGE).

    Returns:
        str: CREATE TABLE query.
    """
    query_for_creating_table = f'CREATE TABLE {table_name} (\n'
    for column in df:
        column = f"{column}"[:25]
        if column in list_num_columns:
            query_for_creating_table += f"""{column} \t number,\n"""
        elif column in list_date_columns:
            query_for_creating_table += f"""{column} \t date,\n"""
        elif column in list_timestamp_columns:
            query_for_creating_table += f"""{column} \t timestamp,\n"""
        elif column in list_clob_columns:
            query_for_creating_table += f"""{column} \t clob,\n"""
        elif column in list_geometry_columns:
            query_for_creating_table += f"""{column} \t sdo_geometry,\n"""
        else:
            query_for_creating_table += f"""{column} \t varchar2({varchar_len}),\n"""
    query_for_creating_table = query_for_creating_table[:-2]
    query_for_creating_table += '\n)'

    # Add partitioning clause if specified
    if partition_column and partition_type:
        partition_type = partition_type.upper()
        # Determine if partition column is date or timestamp for formatting
        is_timestamp = partition_column in list_timestamp_columns
        is_date = partition_column in list_date_columns

        def format_partition_value(val):
            if is_timestamp:
                if not str(val).startswith("TIMESTAMP"):
                    # Format: dd.mm.yyyy hh24:mi:ss
                    try:
                        dt = pd.to_datetime(val)
                        return f"TO_TIMESTAMP('{dt.strftime('%d.%m.%Y %H:%M:%S')}', 'DD.MM.YYYY HH24:MI:SS')"
                    except Exception:
                        return f"TIMESTAMP '{val}'"
                return val
            elif is_date:
                if not str(val).startswith("DATE"):
                    try:
                        dt = pd.to_datetime(val)
                        return f"TO_DATE('{dt.strftime('%d.%m.%Y')}', 'DD.MM.YYYY')"
                    except Exception:
                        return f"DATE '{val}'"
                return val
            return str(val)

        if partition_type == 'RANGE':
            if partition_granularity and partition_start and partition_end:
                # Use automatic interval partitioning (Oracle 11g+)
                if partition_interval:
                    query_for_creating_table += (
                        f"\nPARTITION BY RANGE ({partition_column})\n"
                        f"INTERVAL ({partition_interval}) (\n"
                        f"  PARTITION p_start VALUES LESS THAN ({format_partition_value(partition_start)})\n)"
                    )
                else:
                    # Manual partitions for each period
                    # Remove DATE/TIMESTAMP/TO_DATE/TO_TIMESTAMP prefix for pd.date_range
                    def clean_val(val):
                        for prefix in ["DATE '", "TIMESTAMP '", "TO_DATE('", "TO_TIMESTAMP('"]:
                            if val.startswith(prefix):
                                val = val[len(prefix):]
                                if val.endswith("')"):
                                    val = val[:-2]
                                elif val.endswith("'"):
                                    val = val[:-1]
                        return val
                    start_val = clean_val(str(partition_start))
                    end_val = clean_val(str(partition_end))
                    freq_map = {'MONTH': 'M', 'DAY': 'D', 'HOUR': 'H'}
                    freq = freq_map.get(
                        partition_granularity.upper(), partition_granularity[0])
                    periods = pd.date_range(
                        start=start_val,
                        end=end_val,
                        freq=freq
                    )
                    partitions = []
                    for i, dt in enumerate(periods[1:]):
                        if is_timestamp:
                            val = f"TO_TIMESTAMP('{dt.strftime('%d.%m.%Y %H:%M:%S')}', 'DD.MM.YYYY HH24:MI:SS')"
                        elif is_date:
                            val = f"TO_DATE('{dt.strftime('%d.%m.%Y')}', 'DD.MM.YYYY')"
                        else:
                            val = f"'{dt}'"
                        partitions.append(
                            f"PARTITION p{i+1} VALUES LESS THAN ({val})")
                    query_for_creating_table += (
                        f"\nPARTITION BY RANGE ({partition_column}) (\n  " +
                        ",\n  ".join(partitions) + "\n)"
                    )
            elif partition_values:
                partitions = ',\n  '.join(
                    [f"PARTITION p{i+1} VALUES LESS THAN ({format_partition_value(v)})" for i, v in enumerate(
                        partition_values)]
                )
                query_for_creating_table += f"\nPARTITION BY RANGE ({partition_column}) (\n  {partitions}\n)"
        elif partition_type == 'LIST' and partition_values:
            partitions = ',\n  '.join(
                [f"PARTITION p{i+1} VALUES ({', '.join(map(format_partition_value, vals))})" for i,
                 vals in enumerate(partition_values)]
            )
            query_for_creating_table += f"\nPARTITION BY LIST ({partition_column}) (\n  {partitions}\n)"
        elif partition_type == 'HASH':
            query_for_creating_table += f"\nPARTITION BY HASH ({partition_column})"
        # else: ignore or raise error for unsupported/invalid config

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

def send_file_via_telegram(token, chat_id, path_to_file, proxies=None, captions= None, verbose = False, parse_mode = 'html'):
    files = {
        'document': open(path_to_file, 'rb',),
    }
    data = None
    params = {
        'chat_id': chat_id,
        'parse_mode': parse_mode  # Set the parse mode to Markdown
        }
    if captions:
        data = {'caption': captions}
    response = requests.post(
        f'https://api.telegram.org/bot{token}/sendDocument', files=files, data=data, proxies=proxies, params=params)
    if verbose:
        print(f'"{path_to_file}" has been sent to chat_id: "{chat_id}"')

    return response.json()


def send_photo_via_telegram(token, chat_id, path_to_photo, proxies = None, captions=None, verbose=False, parse_mode='html'):
    files = {
        'photo': open(path_to_photo, 'rb'),
    }
    data = {'chat_id': chat_id, 'parse_mode': parse_mode}
    if captions:
        data['caption'] = captions
    response = requests.post(
        f'https://api.telegram.org/bot{token}/sendPhoto', files=files, data=data, proxies=proxies)
    if verbose:
        print(f'"{path_to_photo}" has been sent to chat_id: "{chat_id}" with caption: "{captions}"')
    
    return response.json()


def send_msg_via_telegram(token, chat_id, msg_text, proxies = None, parse_mode='html', verbose = False):

    params = {
        'chat_id': chat_id,
        'text': msg_text,
        'parse_mode': parse_mode  # Set the parse mode to Markdown
        }
    response = requests.post(
        f'https://api.telegram.org/bot{token}/sendMessage', params=params, proxies=proxies)
    if verbose:
        print(f'"{msg_text}" has been sent to chat_id: "{chat_id}"')

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

def send_email(send_to, send_from, subject, host, content=None, directory=None, file_to_attach=None, cc_to=None, show_logs=False):

    # Create the message
    msg = EmailMessage()
    # f'Contents of directory {os.path.abspath(directory)}'
    msg['Subject'] = subject

    # --- HANDLE "TO" INPUT ---
    # If it's a list or tuple, join it into a comma-separated string
    if isinstance(send_to, (list, tuple)):
        msg['To'] = ', '.join(send_to)
    else:
        msg['To'] = send_to
    # --- HANDLE "CC" INPUT ---
    if cc_to:
        if isinstance(cc_to, (list, tuple)):
            msg['Cc'] = ', '.join(cc_to)
        else:
            msg['Cc'] = cc_to 

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

def measure_time(func):
    def wrapper(*args, **kwargs):
        start = timeit.default_timer()
        result = func(*args, **kwargs)
        stop = timeit.default_timer()
        print(f"executed in {(stop - start) / 60:.2f} min")
        return result
    return wrapper

def pass_encoder(password):
    encoded_password = base64.b64encode(password.encode()).decode()
    return encoded_password

def pass_decoder(encoded_password):
    password_decode = base64.b64decode(encoded_password).decode()
    return password_decode

def voronoi_split(poly_figure, coords_list, buffer_value=2000, boundary_value=100):
    """
    Perform a Voronoi partition on a polygon with given coordinates and intersect these partitions with the original polygon.

    Parameters:
    poly_figure (shapely.geometry.Polygon): The polygon to split.
    coords_list (list of tuples): List of coordinates (x, y) inside the polygon.
    buffer_value (int, optional): Buffer value for the polygon. Default is 2000.
    boundary_value (int, optional): Distance between points on the boundary. Default is 100.

    Returns:
    geopandas.GeoDataFrame: Resulting GeoDataFrame after intersection.
    """

    # Filter coordinates inside the polygon using vectorized operation
    coords_array = np.array(coords_list)
    contained = np.array([poly_figure.contains(Point(p)) for p in coords_array])
    coords_inside = coords_array[contained]

    # Create a polygon boundary with buffer
    bound = poly_figure.buffer(buffer_value).envelope.boundary
    distances = np.arange(0, np.ceil(bound.length), boundary_value)
    boundarypoints = [bound.interpolate(distance=d) for d in distances]
    boundarycoords = np.array([[p.x, p.y] for p in boundarypoints])

    # Create an array of all points
    all_coords = np.concatenate((boundarycoords, coords_inside)) 

    # Perform Voronoi partition
    vor = Voronoi(points=all_coords)
    lines = [shapely.geometry.LineString(vor.vertices[line]) for line in vor.ridge_vertices if -1 not in line]

    polys = shapely.ops.polygonize(lines)
    voronois = gpd.GeoDataFrame(geometry=gpd.GeoSeries(polys), crs="epsg:4326")
    polydf = gpd.GeoDataFrame(geometry=[poly_figure], crs="epsg:4326")

    # Intersect Voronoi partitions with the original polygon
    result = gpd.overlay(df1=voronois, df2=polydf, how="intersection")

    return result

def get_all_cred_dict(vault_url = pass_decoder(os.environ.get(f'VAULT_LINK_URL')), vault_token = pass_decoder(os.environ.get(f'VAULT_TKN')), path_to_secret = pass_decoder(os.environ.get(f'PATH_TO_SECRET_VLT')), mount_point = pass_decoder(os.environ.get(f'MOUNT_POINT_VLT'))):
    '''
    all_cred_dict = get_all_cred_dict(
        vault_url=url, vault_token=token, path_to_secret='xxx', mount_point='xxx')
    '''
    client = hvac.Client(url=vault_url, token=vault_token)
    authenticated_status = client.is_authenticated()
    if authenticated_status:
        raw_resonse = client.secrets.kv.read_secret_version(
            path=path_to_secret, mount_point=mount_point, raise_on_deleted_version=True)
        all_cred_dict = raw_resonse['data']['data']
        return all_cred_dict
    return None


if __name__ == "__main__":
    pass
