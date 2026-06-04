from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("nurtelecom_gras_library")
except PackageNotFoundError:  # running from a source tree that isn't installed
    __version__ = "0.0.0"

from nurtelecom_gras_library.OracleDataRetriever import (
    OracleDataRetriever,
    enable_thick_mode,
)
from nurtelecom_gras_library.OracleGeoDataImporter import OracleGeoDataImporter
from nurtelecom_gras_library.updated_connection import get_db_connection
from nurtelecom_gras_library.JiraServiceDeskClient import JiraClient
from nurtelecom_gras_library.TableauServerManager import TableauServerManager
from nurtelecom_gras_library.additional_functions import (
    get_all_cred_dict,
    make_table_query_from_pandas,
    merge_clob_maker,
    get_list_of_objects,
    get_a_copy,
    value_extractor,
    register_smb_session,
    voronoi_split,
    send_email,
    send_email_html,
    send_msg_via_telegram,
    send_photo_via_telegram,
    send_file_via_telegram,
    send_telegram_msg,
    send_sms,
    error_sender,
    measure_time,
    pass_encoder,
    pass_decoder,
)

__all__ = [
    "__version__",
    # Connections
    "get_db_connection",
    "OracleDataRetriever",
    "OracleGeoDataImporter",
    "enable_thick_mode",
    # Integrations
    "JiraClient",
    "TableauServerManager",
    # Credentials / SQL helpers
    "get_all_cred_dict",
    "make_table_query_from_pandas",
    "merge_clob_maker",
    # Notifications
    "send_email",
    "send_email_html",
    "send_msg_via_telegram",
    "send_photo_via_telegram",
    "send_file_via_telegram",
    "send_telegram_msg",
    "send_sms",
    "error_sender",
    # Filesystem / utilities
    "get_list_of_objects",
    "get_a_copy",
    "value_extractor",
    "register_smb_session",
    "measure_time",
    "pass_encoder",
    "pass_decoder",
    # Spatial
    "voronoi_split",
]
