from logging import exception
from operator import index
import oracledb
import pandas as pd
import geopandas as gpd
import timeit
import os
import shapely.wkt as wkt
from shapely.geometry import MultiPolygon
from sqlalchemy.engine import create_engine
from sqlalchemy import update, text
from nurtelecom_gras_library.OracleDataRetriever import OracleDataRetriever
from nurtelecom_gras_library.additional_functions import measure_time

'most complete version to deal with SHAPE FILES'


class OracleGeoDataImporter(OracleDataRetriever):

    def __init__(self, user, password, host, port='1521', service_name='DWH') -> None:
        super().__init__(user, password, host, port, service_name)

    @measure_time
    def get_data(self, query, use_geopandas=True, geom_columns_list=['geometry'],
                 point_columns_list=[], remove_na=False, show_logs=False, crid='EPSG:4326'):
        """
        Retrieve data from the Oracle database using the provided SQL query.

        Note:
        - To ensure proper geometry extraction, your SQL query must use:
          SDO_UTIL.TO_WKTGEOMETRY(sdo_cs.transform(t.geometry, 4326)) AS geometry
        - Using this is required for correct WKT geometry parsing and GeoDataFrame creation.
        """
        # point_columns_list = point_columns_list or []

        try:
            query = text(query)
            engine = self.get_engine()

            # Using context manager for connection
            with engine.connect() as conn:
                data = pd.read_sql(query, con=conn)
                data.columns = data.columns.str.lower()

                if remove_na:
                    data.dropna(inplace=True)

                if point_columns_list:
                    for column in point_columns_list:
                        data[column] = data[column].apply(
                            lambda x: wkt.loads(str(x)))

                if use_geopandas:
                    # WKT from Oracle is in a proprietary object format.
                    # We need to convert it to string and further convert it to
                    # shapely geometry using wkt.loads. GeoPandas must contain
                    # a "geometry" column, so previous names have to be renamed.
                    # CRS has to be applied to have a proper GeoPandas DataFrame.
                    for geom_column in geom_columns_list:
                        data[geom_column] = data[geom_column].apply(
                            lambda x: wkt.loads(str(x)))
                    # data.rename(
                    #     columns={geom_column: 'geometry'}, inplace=True)
                    data = gpd.GeoDataFrame(data, crs=crid)

            if show_logs:
                print(data.head())

            return data

        except Exception as e:
            print(f"Error during data retrieval: {e}")
            raise


if __name__ == "__main__":

    pass
