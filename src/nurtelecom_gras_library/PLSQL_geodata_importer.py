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
from nurtelecom_gras_library.PLSQL_data_importer import PLSQL_data_importer
from nurtelecom_gras_library.additional_functions import measure_time

'most complete version to deal with SHAPE FILES'


class PLSQL_geodata_importer(PLSQL_data_importer):

    def __init__(self, user, password, host, port='1521', service_name='DWH') -> None:
        super().__init__(user, password, host, port, service_name)

    @measure_time
    def get_data(self, query, use_geopandas=False, geom_columns_list=['geometry'],
                 point_columns_list=[], remove_na=False, show_logs=False):
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
                    '''wkt from the oracle in proprietary object format.
            we need to convert it to string and further converted to 
            shapely geometry using wkt.loads. Geopandas has to contain
            "geometry" column, therefore previous names have to be renamed.
            CRS has to be applied to have proper geopandas dataframe'''
                    for geom_colum in geom_columns_list:
                        data[geom_colum] = data[geom_colum].apply(
                            lambda x: wkt.loads(str(x)))
                    # data.rename(
                    #     columns={geom_column: 'geometry'}, inplace=True)
                    data = gpd.GeoDataFrame(data, crs="EPSG:4326")

            if show_logs:
                print(data.head())

            return data

        except Exception as e:
            print(f"Error during data retrieval: {e}")
            raise


if __name__ == "__main__":

    pass
