import logging
import oracledb
import pandas as pd
from sqlalchemy import text
from nurtelecom_gras_library.OracleDataRetriever import OracleDataRetriever
from nurtelecom_gras_library.additional_functions import measure_time

logger = logging.getLogger(__name__)

'most complete version to deal with SHAPE FILES'


def _parse_geometry(value, geom_format, wkt_module, wkb_module):
    """Convert a single Oracle geometry value into a shapely geometry.

    Handles every form Oracle/oracledb can return: NULL/NaN, an oracledb LOB
    (CLOB for WKT or BLOB for WKB), raw bytes/bytearray/memoryview, a hex
    string, or plain WKT text.

    :param value: The raw cell value from the result set.
    :param geom_format: ``'wkb'``, ``'wkt'``, or ``'auto'`` (bytes -> WKB,
        text -> WKT, decided per value).
    :param wkt_module: The imported ``shapely.wkt`` module.
    :param wkb_module: The imported ``shapely.wkb`` module.
    :return: A shapely geometry, or ``None`` for empty/NULL input.
    """
    # NULL / NaN (guard pd.isna so we never call it on a LOB object).
    if value is None or (not hasattr(value, 'read') and pd.isna(value)):
        return None

    # oracledb LOB (CLOB/BLOB): read its content while the connection is open.
    if hasattr(value, 'read'):
        value = value.read()

    if isinstance(value, memoryview):
        value = value.tobytes()
    elif isinstance(value, bytearray):
        value = bytes(value)

    fmt = geom_format
    if fmt == 'auto':
        fmt = 'wkb' if isinstance(value, bytes) else 'wkt'

    if fmt == 'wkb':
        # A hex-encoded string is also valid WKB input for shapely.
        if isinstance(value, str):
            value = bytes.fromhex(value.strip())
        if not value:
            return None
        return wkb_module.loads(value)

    # WKT
    if isinstance(value, bytes):
        value = value.decode('utf-8', errors='ignore')
    text_val = str(value).strip()
    if not text_val:
        return None
    return wkt_module.loads(text_val)


class OracleGeoDataImporter(OracleDataRetriever):

    def __init__(self, user, password, host, port='1521', service_name='DWH') -> None:
        """Create a geo-aware Oracle connector.

        Same connection parameters as :class:`OracleDataRetriever`; this subclass
        overrides :meth:`get_data` to parse geometry columns into a GeoDataFrame.

        :param user: Database user.
        :param password: Database password.
        :param host: Database host.
        :param port: Listener port (default '1521').
        :param service_name: Oracle service name (default 'DWH').
        """
        super().__init__(user, password, host, port, service_name)

    @measure_time
    def get_data(self, query, use_geopandas=True, geom_columns_list=None,
                 point_columns_list=None, remove_na=False, show_logs=False,
                 crid='EPSG:4326', geom_format='auto'):
        """
        Retrieve spatial data from Oracle and return it as a GeoDataFrame.

        Geometry is read via a standard interchange format. **WKB (binary) is
        preferred** — it is compact, fast, and lossless::

            SELECT SDO_UTIL.TO_WKBGEOMETRY(SDO_CS.TRANSFORM(t.geometry, 4326)) AS geometry
            FROM my_geo_table t

        WKT (text) is also supported::

            SELECT SDO_UTIL.TO_WKTGEOMETRY(SDO_CS.TRANSFORM(t.geometry, 4326)) AS geometry
            FROM my_geo_table t

        ``TO_WKBGEOMETRY`` returns a BLOB and ``TO_WKTGEOMETRY`` a CLOB; both are
        read correctly. NULL geometries map to ``None``. The first column in
        ``geom_columns_list`` becomes the GeoDataFrame's active geometry; any
        others are parsed into shapely objects but left as plain columns.

        :param query: SQL query returning one or more WKB/WKT geometry columns.
        :param use_geopandas: If True, return a GeoDataFrame; otherwise a plain
            DataFrame (geometry columns are still parsed if listed).
        :param geom_columns_list: Geometry column name(s) to parse. Defaults to
            ``['geometry']``. Matched case-insensitively.
        :param point_columns_list: Additional geometry column(s) to parse into
            shapely objects (not set as the active geometry).
        :param remove_na: If True, drop rows containing NULLs before parsing.
        :param show_logs: If True, print the head of the result.
        :param crid: CRS to assign to the GeoDataFrame (default 'EPSG:4326').
        :param geom_format: ``'auto'`` (default; detect WKB bytes vs WKT text
            per value), ``'wkb'``, or ``'wkt'``.
        :return: A ``geopandas.GeoDataFrame`` (or ``pandas.DataFrame`` when
            ``use_geopandas`` is False).
        """
        try:
            import geopandas as gpd
            import shapely.wkt as wkt
            import shapely.wkb as wkb
        except ImportError as e:
            raise ImportError(
                "OracleGeoDataImporter requires the spatial extras. Install with: "
                "pip install nurtelecom_gras_library[geo]"
            ) from e

        geom_format = geom_format.lower()
        if geom_format not in ('auto', 'wkb', 'wkt'):
            raise ValueError(
                f"geom_format must be 'auto', 'wkb', or 'wkt', got {geom_format!r}."
            )

        # Result columns are lower-cased below, so normalize requested names.
        geom_columns_list = [c.lower() for c in (geom_columns_list or ['geometry'])]
        point_columns_list = [c.lower() for c in (point_columns_list or [])]

        def _parse(v):
            return _parse_geometry(v, geom_format, wkt, wkb)

        try:
            query = text(query)
            engine = self.get_engine()

            # Make oracledb return CLOB/BLOB content as str/bytes directly,
            # so geometries don't come back as unreadable LOB objects.
            prev_fetch_lobs = oracledb.defaults.fetch_lobs
            oracledb.defaults.fetch_lobs = False
            try:
                with engine.connect() as conn:
                    data = pd.read_sql(query, con=conn)
                    data.columns = data.columns.str.lower()

                    if remove_na:
                        data.dropna(inplace=True)

                    # Parse any non-primary geometry/point columns.
                    for column in point_columns_list:
                        if column in data.columns:
                            data[column] = data[column].map(_parse)

                    # Parse the geometry column(s).
                    for geom_column in geom_columns_list:
                        if geom_column in data.columns:
                            data[geom_column] = data[geom_column].map(_parse)
            finally:
                oracledb.defaults.fetch_lobs = prev_fetch_lobs

            if use_geopandas:
                # First requested geometry column present becomes the active one.
                primary_geom = next(
                    (c for c in geom_columns_list if c in data.columns), None)
                if primary_geom is None:
                    raise ValueError(
                        f"None of geom_columns_list={geom_columns_list} were found "
                        f"in the query result columns {list(data.columns)}. "
                        "Make sure your SELECT aliases the geometry column "
                        "accordingly (e.g. ... AS geometry)."
                    )
                data = gpd.GeoDataFrame(data, geometry=primary_geom, crs=crid)

            if show_logs:
                logger.info("Retrieved %d rows; head:\n%s", len(data), data.head())

            return data

        except Exception as e:
            logger.error("Error during data retrieval: %s", e)
            raise


if __name__ == "__main__":

    pass
