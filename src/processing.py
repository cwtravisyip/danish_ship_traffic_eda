import pandas as pd 
import geopandas as gpd
import numpy as np
from typing import List, Dict, Union, Set, Literal


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


def return_ship_attr(df: pd.DataFrame, id_type: Literal['MMSI','IMO'],id: int) -> Dict:
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
    return df_ship.reset_index(drop=True).to_dict(orient ='index')


def return_ship_attr_time_variant(df: pd.DataFrame, id_type: Literal['MMSI','IMO'],id: int) -> Dict:
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

    return df_ship.reset_index(drop=True).to_dict(orient ='index')


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

if __name__ == '__main__':
    pass