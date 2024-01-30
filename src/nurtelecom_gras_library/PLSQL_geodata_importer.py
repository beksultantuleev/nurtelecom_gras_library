from logging import exception
from operator import index
# from matplotlib.pyplot import cla
import cx_Oracle
import pandas as pd
import geopandas as gpd
import timeit
import os
import shapely.wkt as wkt
from shapely.geometry import MultiPolygon
from sqlalchemy.engine import create_engine
from sqlalchemy import update
from sqlalchemy import text
from nurtelecom_gras_library.PLSQL_data_importer import PLSQL_data_importer
from nurtelecom_gras_library.additional_functions import measure_time

'most complete version to deal with SHAPE FILES'


class PLSQL_geodata_importer(PLSQL_data_importer):

    def __init__(self, user, password, host, port='1521', service_name='DWH') -> None:
        super().__init__(user, password, host, port, service_name)

    @measure_time
    def get_data(self, query, use_geopandas=False, geom_column='geometry', point_columns=None, remove_column=None, remove_na=False, show_logs=False):
        point_columns = point_columns or []
        remove_column = remove_column or []

        try:
            query = text(query)
            # Using context manager for connection
            with create_engine(self.ENGINE_PATH_WIN_AUTH).connect() as conn:
                data = pd.read_sql(query, con=conn)
                data.columns = data.columns.str.lower()
                data.drop(remove_column, axis=1, inplace=True)

                if remove_na:
                    data.dropna(inplace=True)

                if point_columns:
                    for column in point_columns:
                        data[column] = data[column].apply(
                            lambda x: wkt.loads(str(x)))

                if use_geopandas:
                    data[geom_column] = data[geom_column].apply(
                        lambda x: wkt.loads(str(x)))
                    data.rename(
                        columns={geom_column: 'geometry'}, inplace=True)
                    data = gpd.GeoDataFrame(data, crs="EPSG:4326")

            stop = timeit.default_timer()

            if show_logs:
                print(data.head())

            return data

        except Exception as e:
            print(f"Error during data retrieval: {e}")
            raise

    @measure_time
    def get_data_old(self, query,
                     use_geopandas=False,
                     geom_column='geometry',
                     point_columns=[],
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
        if len(point_columns) != 0:
            for column in point_columns:
                data[column] = data[column].astype(str)
                data[column] = data[column].apply(
                    wkt.loads)
        if use_geopandas:
            '''wkt from the oracle in proprietary object format.
            we need to convert it to string and further converted to 
            shapely geometry using wkt.loads. Geopandas has to contain
            "geometry" column, therefore previous names have to be renamed.
            CRS has to be applied to have proper geopandas dataframe'''
            data[geom_column] = data[geom_column].astype(str)
            # print(data[geom_column])
            data[geom_column] = data[geom_column].apply(
                wkt.loads)  # .apply(MultiPolygon)

            data.rename(columns={geom_column: 'geometry'}, inplace=True)
            data = gpd.GeoDataFrame(data=data, crs="EPSG:4326")
        stop = timeit.default_timer()
        if show_logs:
            print(data.head())
            print(f"end, time is {(stop - start) / 60:.2f} min")
        self.conn.close()
        self.engine.dispose()
        return data


if __name__ == "__main__":

    pass
