import os

import pandas as pd
import geopandas as gpd
import numpy as np
from typing import List, Dict, Union, Set, Literal, Tuple
import warnings
from shapely.geometry import LineString

def return_id_by_ship_type(df_chunks: pd.io.parsers.readers.TextFileReader, id: Literal['MMSI','IMO']) -> Dict[str, Set]:
    """
    Return a unique list of IMO by the ship type
    :params df_chunks: an iterable object each returning a pandas dataframe, instantiated by
                        calling the pd.read_csv function with the chunksize specified.
    """

    # instantiate empty dictionary
    res_list = {}

    def return_unknown_IMO(df: pd.DataFrame)-> None:
        """verbose behaviour when returning list of IMO"""
        # return the number of unknown IMO
        try:
            pct_unknown: np.float64 = df['IMO'].value_counts(normalize=True)['Unknown']
        except KeyError:
            pass
        except Exception:
            raise
    
        try: 
            n_unique_mmsi: float = df[df['IMO']=='Unknown']['MMSI'].unique().shape[0]
        except Exception:
            raise 

        print(f"{pct_unknown:.0%} records have unknown IMO, representing {n_unique_mmsi} unique MMSI.")

    # loop over each chunk
    for df in df_chunks:

        if id == 'IMO':
            return_unknown_IMO(df)
            df = df[df['IMO'] != 'Unknown']

        else:
            pass


        aggregate = df.groupby(['Ship type'])[id].agg(set)

        for ship_type, mmsi in aggregate.items():
            if ship_type not in res_list.keys():
                res_list[ship_type] = mmsi
            else:
                # update the set if the ship type already exist
                res_list[ship_type] = res_list[ship_type].union(mmsi)
    
    return res_list


def return_ship_attr(df: pd.DataFrame, id_type: Literal['MMSI','IMO'],id: int) -> pd.DataFrame:
    """
    Given the id of a ship, return the time-invariant attribute in the dataframe
    :param df: Data frame to subset on
    :param id_type: The ID type between MMSI or IMO used to identify the vessel
    :param id: The ID
    :return:
    """
    # columns required
    cols: List[str] = ['# Timestamp','Type of mobile','MMSI','IMO','Data source type',
                       'Callsign','Name','Ship type','Width','Length','Type of position fixing device','A','B','C','D']
    # attribute required
    attr: List[str] = ['Type of mobile', 'MMSI', 'IMO', 'Callsign', 'Name', 'Ship type', 'Width', 'Length',
                       'Type of position fixing device', 'A', 'B','C', 'D']

    # filter the dataframe in both dimension
    df_ship = df.loc[df[id_type] ==  id,attr].drop_duplicates()

    if df_ship.shape[0] == 0:
        raise KeyError(f'No records found for the ship with {id_type}: {id}')
    elif df_ship.shape[0] > 1:
        warnings.warn(f'More than 1 unique record found for the ship with {id_type}: {id}')

        df_ship = df.loc[df[id_type] ==  id,cols]\
                    .sort_values('# Timestamp')\
                    .drop_duplicates(keep = 'first')
    else:
        # only one unique record on the expectedly time-invariant attribute
        pass
    return df_ship.reset_index(drop=True)


def return_ship_type_attr(df_path: str,ship_type: str, chunksize: int = None) -> pd.DataFrame:
    """
    Given the id of a ship, return the time-invariant attribute in the dataframe
    :param df: Data frame to subset on
    :param id_type: The ID type between MMSI or IMO used to identify the vessel
    :param id: The ID
    :return:
    """

    # attribute required
    attr: List[str] = ['Type of mobile', 'MMSI', 'IMO', 'Callsign', 'Name', 'Ship type', 'Width', 'Length',
                       'Type of position fixing device', 'A', 'B','C', 'D']

    # instantiate empty list to store chunks of df
    list_df = []

    if chunksize:
        df_iter = pd.read_csv(df_path, chunksize = chunksize)
    else:
        df_iter = [pd.read_csv(df_path)]

    for iter, df in enumerate(df_iter): 
        # filter the dataframe in both dimension
        df_ship = df.loc[df['Ship type'] ==  ship_type,attr].drop_duplicates()

        if df_ship.shape[0] == 0:
            warnings.warn(f'No vessel of type {ship_type} in {iter}-th chunk of the df.')
        elif len(df_ship['MMSI'].unique()) != df_ship.shape[0]:
            warnings.warn(f'One or more vessel has its attribute changed')
        else:
            # only one unique record on the expectedly time-invariant attribute
            pass

        list_df.append(df_ship)

    df_concat = pd.concat(list_df).drop_duplicates().reset_index(drop=True)

    if len(df_concat['MMSI'].unique()) != df_concat.shape[0]:
        warnings.warn(f'One or more vesselhas its attribute changed')
    else:
        # only one unique record on the expectedly time-invariant attribute
        pass

    return df_concat


def return_ship_attr_time_variant(df: pd.DataFrame, id_type: Literal['MMSI','IMO'],id: int) -> pd.DataFrame:
    """
    Given the id of a ship, return the attributes that are less likely to change over time
    :param df: Data frame to subset on
    :param id_type: The ID type between MMSI or IMO used to identify the vessel
    :param id: The ID
    :return:
    """
    # columns required
    cols: List[str] = ['# Timestamp','Type of mobile','MMSI','IMO','Data source type',
                       'Navigational status','Heading','Destination','ETA','Cargo type']
    # attribute required
    attr: List[str] = ['MMSI', 'IMO', 'Navigational status','Heading','Destination','ETA','Cargo type']

    # subset the

    # filter the dataframe in both dimension
    df_ship = df.loc[df[id_type] ==  id,attr].drop_duplicates()

    if df_ship.shape[0] == 0:
        raise KeyError(f'No records found for the ship with {id_type}: {id}')
    elif df_ship.shape[0] > 1:

        df_ship = df.loc[df[id_type] ==  id,cols]\
                    .sort_values('# Timestamp')\
                    .drop_duplicates(keep = 'first')
    else:
        # only one unique record on the expectedly time-invariant attribute
        pass

    return df_ship.reset_index(drop=True)


def return_ship_geo_attr(df: pd.DataFrame, id_type: Literal['MMSI', 'IMO'], id: int) -> gpd.GeoDataFrame:
    """
    Given the id of a ship, return the attributes that are usually time-variant.
    :param df: Data frame to subset on
    :param id_type: The ID type between MMSI or IMO used to identify the vessel
    :param id: The ID
    :return: gpd.GeoDataFrame
    """
    # columns required
    cols: List[str] = ['# Timestamp', 'Latitude', 'Longitude', 'ROT', 'SOG', 'COG', 'Draught', 'Data source type']

    # filter the dataframe in both dimension
    df_ship = df.loc[df[id_type] == id, cols].drop_duplicates()\
                .sort_values('# Timestamp')\
                .reset_index(drop = True)

    if df_ship.shape[0] == 0:
        raise KeyError(f'No records found for the ship with {id_type}: {id}')

    else:

        gdf = gpd.GeoDataFrame(df_ship, geometry = gpd.points_from_xy(
                                    x = df_ship['Longitude'],
                                    y = df_ship['Latitude']))\
                .drop(columns = ['Longitude','Latitude'])

    return gdf

def get_ship_route(path:str, id_type: Literal['MMSI', 'IMO'], id: int, chunk_size:int = 5000) \
        -> Tuple[Union[Dict, gpd.GeoDataFrame]]:

    df_iter = pd.read_csv(path, chunksize = chunk_size)

    # instantiate empty result
    dfs_attr:List[pd.DataFrame] = []
    dfs_attr_tv:List[pd.DataFrame] = []
    gdfs: List[gpd.GeoDataFrame] = []

    for df in df_iter:
        try:
            dfs_attr.append(return_ship_attr(df=df,id_type=id_type,id=id))
            dfs_attr_tv.append(return_ship_attr_time_variant(df=df, id_type=id_type, id=id))
            gdfs.append(return_ship_geo_attr(df=df, id_type=id_type, id=id))
        except KeyError:
            continue
        except Exception:
            raise

    del df_iter
    del df

    df_attr: pd.DataFrame = pd.concat(dfs_attr).drop_duplicates().reset_index(drop=True)
    df_attr_tv: pd.DataFrame = pd.concat(dfs_attr_tv).drop_duplicates().reset_index(drop=True)

    gdf:gpd.GeoDataFrame = pd.concat(gdfs).reset_index(drop= True)

    return df_attr, df_attr_tv, gdf

def return_line_from_points(gdf:gpd.GeoDataFrame)->LineString:
    gdf["# Timestamp"] = gpd.pd.to_datetime(gdf["# Timestamp"])

    # Sort by timestamp
    gdf_sorted = gdf.sort_values("# Timestamp")

    # Create LineString from ordered points
    try:
        line = LineString(gdf_sorted.geometry.tolist())

    except:
        raise
    # Convert to a new GeoDataFrame

    return line


if __name__ == '__main__':
    pass