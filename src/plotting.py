import matplotlib.pyplot as plt 
import matplotlib
import geopandas as gpd
import warnings
import plotly.graph_objects as go
import os

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

def plot_interactive_map(gdf:gpd.GeoDataFrame,destination: str = "./asset/plot_map.html"):
    """
    Take a GeoDataFrame of shipping activity of a marine vessel and output plotly html map
    """
    # validate input data
    assert gdf.crs == 4326,"CRS must be in EPSG 4326"
    assert set(["# Timestamp", "geometry"]).difference(gdf.columns) == 0, \
        "Timestamp column and or geometry column no exists."

    # create interactive map
    fig = go.Figure(go.Scattermap(
    mode = "markers+lines",
    lon = gdf.geometry.x,
    lat = gdf.geometry.y,
    hovertext = gdf["# Timestamp"],
    hovertemplate=  "%{hovertext|%Y-%m-%d %H:%M}<br>%{lat:.1f},%{lon:.1f}",
    marker = {'size': 10}))

    fig.update_layout(
        margin ={'l':0,'t':0,'b':0,'r':0},
        map = {
            'center': {'lon': 11.5, 'lat': 56},
            'style': "light",
            'zoom': 6})

    if os.path.exists(destination):
        warnings.warn(f"{destination} already exists. It will be overwritten")

    fig.write_html(destination)

