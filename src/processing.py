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