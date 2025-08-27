import matplotlib.pyplot as plt 
import matplotlib
import geopandas as gpd
import warnings

def plot_basemap(gdf: gpd.GeoDataFrame, ax:matplotlib.axes._axes.Axes=None):

    try: 
        assert gdf.crs is not None
    except AssertionError:
        warnings.warn("The gdf\'s crs has not been specified.")
    

    if ax is None: 
        print('instantiate new axis object')
        ax = gdf[gdf['FORMAL_EN'].str.contains('Denmark')]\
                .plot(color = "#A0A0A0")
    else:
        gdf[gdf['FORMAL_EN'].str.contains('Denmark')]\
                .plot(color = "#A0A0A0",ax=ax)
        
    
    gdf[~gdf['FORMAL_EN'].str.contains('Denmark')]\
                .plot(color = "#CDCDCD", ax = ax)
    
    return ax

