import os
import shutil
import smtplib
import mimetypes
import functools
import logging
import email.utils
from email.message import EmailMessage
import timeit
import requests
import base64
import hvac
import pandas as pd

logger = logging.getLogger(__name__)


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

def register_smb_session(username, password, smb_server):
    """Register an SMB session for accessing network shares.

    Requires the optional ``smbprotocol`` package
    (``pip install nurtelecom_gras_library[smb]``).
    """
    try:
        import smbclient
    except ImportError as e:
        raise ImportError(
            "register_smb_session requires the 'smbprotocol' package. "
            "Install it with: pip install nurtelecom_gras_library[smb]"
        ) from e
    try:
        smbclient.reset_connection_cache()
        smbclient.register_session(
            smb_server,
            username=username,
            password=password,
        )
        logger.info("SMB session registered successfully")
    except Exception as e:
        logger.error("Failed to register SMB session: %s", e)


def value_extractor(pattern, path):
    """Return the first numeric value following ``pattern`` in a text file.

    Scans ``path`` line by line; for the first line containing ``pattern``,
    the text after the pattern is parsed as a float and returned. Returns
    ``None`` if no line matches.

    :param pattern: Substring that prefixes the value to extract.
    :param path: Path to the text file to scan.
    :return: The parsed float value, or ``None`` if not found.
    """
    with open(path) as f:
        lines = f.read().splitlines()
        for i in lines:
            if pattern in i:
                value = float(i[len(pattern):])
                return value

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
            """Format a single partition bound as an Oracle DATE/TIMESTAMP literal."""
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
                        """Strip Oracle DATE/TIMESTAMP wrapper syntax to a bare date string."""
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

def _as_recipient_list(receiver):
    """Normalize a single recipient or an iterable of recipients to a list."""
    if isinstance(receiver, str):
        return [receiver]
    return list(receiver)


def send_telegram_msg(payload, receiver, database_connector):
    """Queue a Telegram message through the Oracle ``kpi_bot`` package.

    Unlike :func:`send_msg_via_telegram`, this does not call Telegram directly;
    it inserts the message via the ``kpi_bot.tb_message_insert`` stored
    procedure using a database connection. Values are passed as bind
    parameters (no string interpolation / SQL injection).

    :param payload: Message text.
    :param receiver: A single recipient id (str) or a list of recipient ids.
    :param database_connector: An object exposing
        ``.execute(query, params=...)`` (e.g. :class:`OracleDataRetriever`).
    """
    query_for_msg = (
        "BEGIN kpi_bot.tb_message_insert(:receiver, :payload); END;"
    )
    for rec in _as_recipient_list(receiver):
        database_connector.execute(
            query_for_msg, params={"receiver": rec, "payload": payload})


def _telegram_post(token, method, params=None, files=None, data=None, proxies=None):
    """POST to the Telegram Bot API and return the parsed JSON response.

    :param token: Telegram bot token.
    :param method: Bot API method name (e.g. 'sendMessage', 'sendPhoto').
    :param params: Optional query params.
    :param files: Optional files mapping for multipart uploads.
    :param data: Optional form data.
    :param proxies: Optional requests-style proxies dict.
    :return: The parsed JSON response from Telegram.
    """
    response = requests.post(
        f'https://api.telegram.org/bot{token}/{method}',
        params=params, files=files, data=data, proxies=proxies,
    )
    return response.json()


def send_file_via_telegram(token, chat_id, path_to_file, proxies=None, captions=None, verbose=False, parse_mode='html'):
    """Send a document/file to a Telegram chat via the Bot API.

    :param token: Telegram bot token.
    :param chat_id: Target chat identifier.
    :param path_to_file: Local path of the file to send.
    :param proxies: Optional requests-style proxies dict.
    :param captions: Optional caption text for the file.
    :param verbose: If True, log a confirmation message.
    :param parse_mode: Telegram parse mode for the caption ('html', 'Markdown').
    :return: The parsed JSON response from Telegram.
    """
    params = {'chat_id': chat_id, 'parse_mode': parse_mode}
    data = {'caption': captions} if captions else None
    with open(path_to_file, 'rb') as fp:
        result = _telegram_post(
            token, 'sendDocument', params=params,
            files={'document': fp}, data=data, proxies=proxies)
    if verbose:
        logger.info('"%s" has been sent to chat_id: "%s"', path_to_file, chat_id)
    return result


def send_photo_via_telegram(token, chat_id, path_to_photo, proxies=None, captions=None, verbose=False, parse_mode='html'):
    """Send a photo to a Telegram chat via the Bot API.

    :param token: Telegram bot token.
    :param chat_id: Target chat identifier.
    :param path_to_photo: Local path of the image to send.
    :param proxies: Optional requests-style proxies dict.
    :param captions: Optional caption text for the photo.
    :param verbose: If True, log a confirmation message.
    :param parse_mode: Telegram parse mode for the caption ('html', 'Markdown').
    :return: The parsed JSON response from Telegram.
    """
    data = {'chat_id': chat_id, 'parse_mode': parse_mode}
    if captions:
        data['caption'] = captions
    with open(path_to_photo, 'rb') as fp:
        result = _telegram_post(
            token, 'sendPhoto', files={'photo': fp}, data=data, proxies=proxies)
    if verbose:
        logger.info('"%s" has been sent to chat_id: "%s" with caption: "%s"',
                    path_to_photo, chat_id, captions)
    return result


def send_msg_via_telegram(token, chat_id, msg_text, proxies=None, parse_mode='html', verbose=False):
    """Send a text message to a Telegram chat via the Bot API.

    :param token: Telegram bot token.
    :param chat_id: Target chat identifier.
    :param msg_text: Message text to send.
    :param proxies: Optional requests-style proxies dict.
    :param parse_mode: Telegram parse mode ('html', 'Markdown').
    :param verbose: If True, log a confirmation message.
    :return: The parsed JSON response from Telegram.
    """
    params = {'chat_id': chat_id, 'text': msg_text, 'parse_mode': parse_mode}
    result = _telegram_post(token, 'sendMessage', params=params, proxies=proxies)
    if verbose:
        logger.info('"%s" has been sent to chat_id: "%s"', msg_text, chat_id)
    return result


def send_sms(payload, receiver, database_connector):
    """Queue an SMS through the Oracle ``kpi.kpi_sms_to_send`` procedure.

    Values are passed as bind parameters (no string interpolation / SQL
    injection).

    :param payload: SMS text.
    :param receiver: A single MSISDN (str) or a list of MSISDNs.
    :param database_connector: An object exposing
        ``.execute(query, params=...)`` (e.g. :class:`OracleDataRetriever`).
    """
    query_for_msg = (
        "BEGIN kpi.kpi_sms_to_send(msisdn => :msisdn, sms_txt => :sms_txt); "
        "COMMIT; END;"
    )
    for rec in _as_recipient_list(receiver):
        database_connector.execute(
            query_for_msg, params={"msisdn": rec, "sms_txt": payload})

def get_a_copy(path_to_original_file, end_path):
    """Copy a file from one path to another.

    :param path_to_original_file: Source file path.
    :param end_path: Destination file path.
    :raises OSError: If the copy fails.
    """
    try:
        shutil.copyfile(path_to_original_file, end_path)
        logger.info('copy complete: %s -> %s', path_to_original_file, end_path)
    except OSError as e:
        logger.error('copy failed (%s -> %s): %s', path_to_original_file, end_path, e)
        raise


def _format_addresses(value):
    """Join a list/tuple of addresses into a comma-separated header value."""
    if isinstance(value, (list, tuple)):
        return ', '.join(value)
    return value


def send_email(send_to, send_from, subject, host, content=None, html=None,
               directory=None, file_to_attach=None, cc_to=None, show_logs=False,
               port=25, use_tls=False, username=None, password=None):
    """Send an e-mail (plain text and/or HTML) over SMTP, optionally with attachments.

    When ``html`` is provided, a ``multipart/alternative`` message is built with
    ``content`` (or a default) as the plain-text fallback, so non-HTML clients
    still show readable text.

    :param send_to: Recipient address, or a list/tuple of addresses.
    :param send_from: Sender address.
    :param subject: Message subject line.
    :param host: SMTP server host.
    :param content: Optional plain-text body (used as the fallback when ``html`` is set).
    :param html: Optional HTML body.
    :param directory: Optional directory; every file inside is attached.
    :param file_to_attach: Optional single file path to attach.
    :param cc_to: Optional Cc address, or a list/tuple of addresses.
    :param show_logs: If True, log a confirmation after sending.
    :param port: SMTP port (default 25).
    :param use_tls: If True, issue STARTTLS before sending.
    :param username: Optional SMTP username (triggers AUTH login).
    :param password: Optional SMTP password.
    """
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = send_from
    msg['To'] = _format_addresses(send_to)
    if cc_to:
        msg['Cc'] = _format_addresses(cc_to)
    msg['Date'] = email.utils.formatdate(localtime=True)
    msg['Message-ID'] = email.utils.make_msgid()

    # Body: plain text, with an HTML alternative when provided.
    if html is not None:
        msg.set_content(content or 'This email is HTML-formatted. '
                                   'Please view it in an HTML-capable mail client.')
        msg.add_alternative(html, subtype='html')
    elif content is not None:
        msg.set_content(content)

    # Attachments.
    if file_to_attach is not None:
        _attach_file(msg, file_to_attach)
    if directory is not None:
        for name in os.listdir(directory):
            path = os.path.join(directory, name)
            if os.path.isfile(path):
                _attach_file(msg, path)

    with smtplib.SMTP(host, port=port) as s:
        if use_tls:
            s.starttls()
        if username:
            s.login(username, password)
        s.send_message(msg)
    if show_logs:
        logger.info('email sent from >> %s to >> %s', send_from, send_to)


def send_email_html(send_to, send_from, subject, host, html, text=None,
                    cc_to=None, file_to_attach=None, directory=None,
                    show_logs=False, **kwargs):
    """Send an HTML e-mail with a plain-text fallback (wrapper around :func:`send_email`).

    Retained for backward compatibility; equivalent to calling
    :func:`send_email` with ``html=...`` and ``content=text``.

    :param html: HTML body of the message.
    :param text: Optional plain-text fallback body.
    See :func:`send_email` for the remaining parameters (and ``port``/``use_tls``/
    ``username``/``password`` via ``**kwargs``).
    """
    return send_email(
        send_to=send_to, send_from=send_from, subject=subject, host=host,
        content=text, html=html, directory=directory,
        file_to_attach=file_to_attach, cc_to=cc_to, show_logs=show_logs,
        **kwargs)


def _attach_file(msg, path):
    """Attach a single file to an EmailMessage, guessing its MIME type.

    :param msg: The :class:`email.message.EmailMessage` to attach to.
    :param path: Path of the file to attach.
    """
    ctype, encoding = mimetypes.guess_type(path)
    if ctype is None or encoding is not None:
        ctype = 'application/octet-stream'
    maintype, subtype = ctype.split('/', 1)
    filename = os.path.basename(path)
    with open(path, 'rb') as fp:
        msg.add_attachment(fp.read(), maintype=maintype, subtype=subtype, filename=filename)


def error_sender(exception_error, dir_path, project_name, list_of_phone_numbers, database_connector):
    """Format an exception and send it as a Telegram alert via the DB bot.

    :param exception_error: The exception/error to report (truncated to 3500 chars).
    :param dir_path: Path/location where the error occurred (shown in the message).
    :param project_name: Name of the project, used in the alert header.
    :param list_of_phone_numbers: Recipient id(s) passed to :func:`send_telegram_msg`.
    :param database_connector: An object exposing ``.execute(query)``.
    """
    # dir_path = os.path.dirname(os.path.realpath(__file__))
    full_error_msg = f"""Warning!\nError occurred in "{project_name}" at <code>{dir_path}</code>\n
    """ + str(exception_error)[:3500].replace("'", '"')
    send_telegram_msg(payload=full_error_msg, receiver=list_of_phone_numbers,
                      database_connector=database_connector)

def measure_time(func):
    """Decorator that prints the wall-clock runtime (in minutes) of ``func``.

    :param func: The function to wrap.
    :return: The wrapped function, which returns ``func``'s original result.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = timeit.default_timer()
        result = func(*args, **kwargs)
        stop = timeit.default_timer()
        logger.info("%s executed in %.2f min", func.__name__, (stop - start) / 60)
        return result
    return wrapper

def pass_encoder(password):
    """Base64-encode a string. NOTE: this is encoding, not encryption.

    :param password: Plain string to encode.
    :return: The base64-encoded string.
    """
    encoded_password = base64.b64encode(password.encode()).decode()
    return encoded_password

def pass_decoder(encoded_password):
    """Base64-decode a string produced by :func:`pass_encoder`.

    :param encoded_password: The base64-encoded string.
    :return: The decoded plain string.
    """
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
    try:
        import numpy as np
        import shapely
        import shapely.ops
        from shapely.geometry import Point
        import geopandas as gpd
        from scipy.spatial import Voronoi
    except ImportError as e:
        raise ImportError(
            "voronoi_split requires the spatial extras. Install with: "
            "pip install nurtelecom_gras_library[geo]"
        ) from e

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

def _resolve_from_env(value, env_var):
    """Return ``value`` if set, else the base64-decoded env var (or None)."""
    if value is not None:
        return value
    raw = os.environ.get(env_var)
    return pass_decoder(raw) if raw is not None else None


def _unpack_approle(approle):
    """Extract ``(role_id, secret_id)`` from a dict or pair, else ``(None, None)``."""
    if not approle:
        return None, None
    if isinstance(approle, dict):
        if len(approle) != 1:
            raise ValueError(
                "approle dict must contain exactly one {role_id: secret_id} pair")
        ((role_id, secret_id),) = approle.items()
        return role_id, secret_id
    if isinstance(approle, (list, tuple)) and len(approle) == 2:
        return approle[0], approle[1]
    raise ValueError(
        "approle must be a {role_id: secret_id} dict or a (role_id, secret_id) pair")


def get_all_cred_dict(vault_url=None, vault_token=None, path_to_secret=None,
                      mount_point=None, role_id=None, secret_id=None,
                      approle=None, approle_mount_point='approle'):
    '''
    Read a KV v2 secret from HashiCorp Vault and return its data dict.

    Supports two authentication methods:

    * **AppRole** (recommended) — provide a role id and secret id either as
      separate arguments or as a single ``{role_id: secret_id}`` dict::

          creds = get_all_cred_dict(
              vault_url=url, role_id='d2100dc3-...', secret_id='59cf260c-...',
              path_to_secret='xxx', mount_point='xxx')

          # equivalent dict form:
          creds = get_all_cred_dict(
              vault_url=url, approle={'d2100dc3-...': '59cf260c-...'},
              path_to_secret='xxx', mount_point='xxx')

    * **Token** — pass ``vault_token``.

    AppRole takes precedence when both a role id and secret id are available.

    Any argument left as ``None`` is read (base64-decoded) from the matching
    environment variable: ``VAULT_LINK_URL``, ``VAULT_TKN``, ``VAULT_ROLE_ID``,
    ``VAULT_SECRET_ID``, ``PATH_TO_SECRET_VLT``, ``MOUNT_POINT_VLT``.

    :param vault_url: Vault server URL.
    :param vault_token: Static Vault token (token auth).
    :param path_to_secret: Path of the KV v2 secret to read.
    :param mount_point: KV v2 secrets-engine mount point.
    :param role_id: AppRole role id.
    :param secret_id: AppRole secret id.
    :param approle: Convenience for AppRole creds as a single
        ``{role_id: secret_id}`` dict or ``(role_id, secret_id)`` pair.
        Explicit ``role_id``/``secret_id`` arguments take precedence over this.
    :param approle_mount_point: Mount point of the AppRole auth method
        (default ``'approle'``).
    :return: The secret's ``data`` dict.
    :raises ValueError: If no usable credentials are provided.
    :raises RuntimeError: If authentication with Vault fails.
    '''
    vault_url = _resolve_from_env(vault_url, 'VAULT_LINK_URL')
    vault_token = _resolve_from_env(vault_token, 'VAULT_TKN')
    role_id = _resolve_from_env(role_id, 'VAULT_ROLE_ID')
    secret_id = _resolve_from_env(secret_id, 'VAULT_SECRET_ID')
    path_to_secret = _resolve_from_env(path_to_secret, 'PATH_TO_SECRET_VLT')
    mount_point = _resolve_from_env(mount_point, 'MOUNT_POINT_VLT')

    # Fill any missing AppRole creds from the convenience `approle` argument.
    approle_rid, approle_sid = _unpack_approle(approle)
    role_id = role_id or approle_rid
    secret_id = secret_id or approle_sid

    client = hvac.Client(url=vault_url)
    if role_id and secret_id:
        # AppRole login: hvac sets client.token from the returned auth.
        client.auth.approle.login(
            role_id=role_id, secret_id=secret_id, mount_point=approle_mount_point)
    elif vault_token:
        client.token = vault_token
    else:
        raise ValueError(
            "No Vault credentials provided: pass either 'vault_token' or both "
            "'role_id' and 'secret_id' (directly or via the corresponding "
            "environment variables).")

    if not client.is_authenticated():
        raise RuntimeError("Vault authentication failed.")

    raw_response = client.secrets.kv.read_secret_version(
        path=path_to_secret, mount_point=mount_point, raise_on_deleted_version=True)
    return raw_response['data']['data']


if __name__ == "__main__":
    pass
