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

'most complete version to deal with SHAPE FILES'


class PLSQL_geodata_importer(PLSQL_data_importer):

    # def __init__(self, user, password, host, port='1521', service_name='dwh') -> None:
    #     super().__init__(user, password, host, port, service_name)
    def __init__(self, user, password, host, port='1521', service_name='DWH') -> None:
        super().__init__(user, password, host, port, service_name)

    def get_data(self, query,
                 use_geopandas=False,
                 geom_column='geometry',
                 point_columns=[],
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
