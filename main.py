import streamlit as st
import pandas as pd
import pydeck as pdk
import branca.colormap as cm
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from typing import List
import json
from PIL import Image
image = Image.open('./favicon.png')
st.set_page_config(page_title="H3: Simplifying the World's Map", page_icon=image)
st.header("H3: Simplifying the World's Map", divider="rainbow")

session = Session.builder.configs(st.secrets["geodemo"]).create()
# st.set_page_config(page_title="H3 in Streamlit", layout="wide")

st.subheader("What is H3")

col1, col2 = st.columns([0.7, 0.3])
with col1:
    st.markdown("H3 Discrete Global Grid is a way to divide the world into a grid of hexagonal cells of equal sizes, "
            "each with a unique identifier (string or integer). It is a hierarchical grid, meaning that cells can be "
            "aggregated into larger cells, and vice versa. This makes it very efficient for processing geospatial data.") 
with col2:
    st.image('https://viennadatasciencegroup.at/post/2019-11-21-h3spark/featured.png', 
         width=180)
    
col1, col2, col3, col4 = st.columns(4)
with col2:
    st.write("**String**")
    st.text('8c274daeb7a0bff')
    st.text('8c2ab2d9294c5ff')
    st.text('8c2ab2da36605ff')

with col3:
    st.write("**Integer**")
    st.text('631195381387627519')
    st.text('631255110006392319')
    st.text('631255110288541183')

st.markdown("The lowest resolution is 0, at which the world is divided into 122 hexagons. "
            "The highest resolution is 15, at which the size of a hexagon is less than a square meter, "
            "and the world can be divided into approximately 600 trillion hexagons."
            " You can check different resolutions and play with hierarchy levels using the widget below. Hover on hexagons to see their IDs.")

# ------ Visualisation 1 ---------
@st.cache_resource
def get_h3point_df(resolution: float, row_count: int) -> pd.DataFrame:
    return session.sql(
        f"select distinct h3_point_to_cell_string(ST_POINT(UNIFORM( -180 , 180 , random()), UNIFORM( -90 , 90 , random())), {resolution}) as h3 from table(generator(rowCount => {row_count}))"
    ).to_pandas()


@st.cache_resource
def get_coverage_layer(df: pd.DataFrame, line_color: List) -> pdk.Layer:
    return pdk.Layer(
        "H3HexagonLayer",
        df,
        get_hexagon="H3",
        stroked=True,
        filled=False,
        auto_highlight=True,
        elevation_scale=45,
        pickable=True,
        extruded=False,
        get_line_color=line_color,
        line_width_min_pixels=1,
    )

min_v_1, max_v_1, v_1, z_1, lon_1, lat_1 = ( 0, 2, 0, 1, 0.9982847947205775, 2.9819747220001886,)
col1, col2 = st.columns([70, 30])
with col1:
    h3_resolut_1 = st.slider(
        "H3 resolution", min_value=min_v_1, max_value=max_v_1, value=v_1)

with col2:
    levels_option = st.selectbox("Levels", ("One", "Two", "Three"))

df = get_h3point_df(h3_resolut_1, 100000)
layer_coverage_1 = get_coverage_layer(df, [36, 191, 242])

visible_layers_coverage_1 = [layer_coverage_1]

if levels_option == "Two":
    df_coverage_level_1 = get_h3point_df(h3_resolut_1 + 1, 100000)
    layer_coverage_1_level_1 = get_coverage_layer(df_coverage_level_1, [217, 102, 255])
    visible_layers_coverage_1 = [layer_coverage_1, layer_coverage_1_level_1]

if levels_option == "Three":
    df_coverage_level_1 = get_h3point_df(h3_resolut_1 + 1, 100000)
    layer_coverage_1_level_1 = get_coverage_layer(df_coverage_level_1, [217, 102, 255])

    df_coverage_level_2 = get_h3point_df(h3_resolut_1+2, 1000000)

    layer_coverage_1_level2 = get_coverage_layer(df_coverage_level_2, [18, 100, 129])
    visible_layers_coverage_1 = [
        layer_coverage_1,
        layer_coverage_1_level_1,
        layer_coverage_1_level2, ]

st.pydeck_chart(
    pdk.Deck(map_provider='carto', 
        map_style='light',
        initial_view_state=pdk.ViewState(
            latitude=lat_1, longitude=lon_1, zoom=z_1, height=400
        ),
        tooltip={"html": "<b>ID:</b> {H3}", "style": {"color": "white"}},
        layers=visible_layers_coverage_1,
    )
)
# ------ Visualisation 1 End ---------
st.divider()
st.subheader("H3 in Snowflake")
st.write("""
    In Snowflake, we have implemented nineteen H3 functions. The most popular ones among our customers are those 
    that convert a Latitude/Longitude pair or a GEOGRAPHY Point to H3 Cell ID ([H3_LATLNG_TO_CELL](https://docs.snowflake.com/sql-reference/functions/h3_latlng_to_cell) 
    and [H3_POINT_TO_CELL](https://docs.snowflake.com/sql-reference/functions/h3_point_to_cell)), 
    get the boundary of an H3 Cell ([H3_CELL_TO_BOUNDARY](https://docs.snowflake.com/sql-reference/functions/h3_cell_to_boundary)), 
    or obtain the coordinates of the centers of H3 Cells ([H3_CELL_TO_POINT](https://docs.snowflake.com/sql-reference/functions/h3_cell_to_point)). 
    However, they also use more complex functions, namely H3_COVERAGE and H3_POLYGON_TO_CELLS.""")
st.write("""
    [H3_COVERAGE](https://docs.snowflake.com/sql-reference/functions/h3_coverage) provides full coverage of a polygon with H3 cells, meaning that 
    it returns all the H3 cells that intersect with the given polygon. 
    This is particularly useful when you need a comprehensive representation of a spatial area.
     Additionaly, just like Snowflake's GEOGRAPHY data type, H3_COVERAGE utilizes spherical geometry for its calculations. 
     It means it considers the ellipsoidal nature of the Earth when determining which H3 cells intersect with a given polygon.
    This is especialy important when you work with spatial objects on the global scale, e.g tessellate shapes of countries.""")
st.write("""
    In contrast, the [H3_POLYGON_TO_CELLS](https://docs.snowflake.com/sql-reference/functions/h3_polygon_to_cells)
    function is centroid-based. It returns the H3 cells whose centroids are within the specified polygon. 
    And unlike H3_COVERAGE it operates on planar geometry.
     It assumes a flat surface for its calculations, which simplifies the processing but can introduce distortions, especially over large areas.""")
st.write("""
    Explore the differences between these two functions at both global and local scales using the provided widget.
    Compare the functions' results against the initial (Light Purple) polygon to understand their behavior.""")


# ------ Visualisation 2 ---------
col1, col2, col3 = st.columns(3)

with col1:
    poly_scale_2 = st.selectbox("Scale of polygon", ("Global", "Local"), index=1)
    if poly_scale_2 == 'Global':
        min_v_2, max_v_2, v_2, z_2, lon_2, lat_2 = 2, 5, 4, 2, -94.50284957885742, 38.51405689475766
    else:
        min_v_2, max_v_2, v_2, z_2, lon_2, lat_2 = 7, 10, 9, 9, -73.98452997207642, 40.74258515841464

with col2:
    original_shape_2 = st.selectbox("Show original shape", ("Yes", "No"),  index=0)

with col3:
    h3_res_2 = st.slider( "H3 resolution ", min_value=min_v_2, max_value=max_v_2, value=v_2)

@st.cache_resource
def get_df_shape_2(poly_scale_2: str) -> pd.DataFrame:
    df = session.sql(
        f"select geog from snowpublic.streamlit.h3_polygon_spherical where scale_of_polygon = '{poly_scale_2}'"
    ).to_pandas()
    df["coordinates"] = df["GEOG"].apply(lambda row: json.loads(row)["coordinates"][0])
    return df


@st.cache_resource
def get_layer_shape_2(df: pd.DataFrame, line_color: List) -> pdk.Layer:
    return pdk.Layer("PolygonLayer", 
                     df, 
                     opacity=0.9, 
                     stroked=True, 
                     get_polygon="coordinates",
                     filled=False,
                     extruded=False,
                     wireframe=True,
                     get_line_color=line_color,
                     line_width_min_pixels=1)

@st.cache_resource
def get_df_coverage_2(h3_res_2: float, poly_scale_2: str) -> pd.DataFrame:
    return session.sql(
        f"select value::string as h3 from snowpublic.streamlit.h3_polygon_planar, TABLE(FLATTEN(h3_coverage_strings(geog, {h3_res_2}))) where scale_of_polygon = '{poly_scale_2}'"
    ).to_pandas()

@st.cache_resource
def get_layer_coverage_2(df_coverage_2: pd.DataFrame, line_color: List) -> pdk.Layer:
    return pdk.Layer("H3HexagonLayer", 
                     df_coverage_2, 
                     get_hexagon="H3", 
                     extruded=False,
                     stroked=True, 
                     filled=False, 
                     get_line_color=line_color, 
                     line_width_min_pixels=1)

@st.cache_resource
def get_df_polyfill_2(h3_res_2: float, poly_scale_2: str) -> pd.DataFrame:
    return session.sql(
        f"select value::string as h3 from snowpublic.streamlit.h3_polygon_planar, TABLE(FLATTEN(h3_polygon_to_cells_strings(geog, {h3_res_2}))) where scale_of_polygon = '{poly_scale_2}'"
    ).to_pandas()

@st.cache_resource
def get_layer_polyfill_2(df_polyfill_2: pd.DataFrame, line_color: List) -> pdk.Layer:
    return pdk.Layer("H3HexagonLayer", 
                     df_polyfill_2, 
                     get_hexagon="H3", 
                     extruded=False,
                     stroked=True, 
                     filled=False, 
                     get_line_color=line_color, 
                     line_width_min_pixels=1)

df_shape_2 = get_df_shape_2(poly_scale_2)
layer_shape_2 = get_layer_shape_2(df_shape_2, [217, 102, 255])

df_coverage_2 = get_df_coverage_2(h3_res_2, poly_scale_2)
layer_coverage_2 = get_layer_coverage_2(df_coverage_2, [18, 100, 129])

df_polyfill_2 = get_df_polyfill_2(h3_res_2, poly_scale_2)
layer_polyfill_2 = get_layer_polyfill_2(df_polyfill_2, [36, 191, 242])

if original_shape_2 == "Yes":
    visible_layers_coverage_2 = [layer_coverage_2, layer_shape_2]
    visible_layers_polyfill_2 = [layer_polyfill_2, layer_shape_2]
else:
    visible_layers_coverage_2 = [layer_coverage_2]
    visible_layers_polyfill_2 = [layer_polyfill_2]

col1, col2 = st.columns(2)

with col1:
    st.pydeck_chart(pdk.Deck(map_provider='carto', map_style='light',
                             initial_view_state=pdk.ViewState(
                                 latitude=lat_2,
                                 longitude=lon_2, 
                                 zoom=z_2, 
                                 width = 350, 
                                 height = 250),
                             layers=visible_layers_coverage_2))
    st.caption('H3_COVERAGE')

with col2:
    st.pydeck_chart(pdk.Deck(map_provider='carto', map_style='light',
                             initial_view_state=pdk.ViewState(
                                 latitude=lat_2,
                                 longitude=lon_2,
                                 zoom=z_2, 
                                 width = 350, 
                                 height = 250),
                             layers=visible_layers_polyfill_2))
    st.caption('H3_POLYGON_TO_CELLS')

# ------ Visualisation 2 End ---------
st.divider()

st.header("Use Cases")

st.markdown("Many companies use H3 grid today. Some of them use it for analytical and machine learning use cases"
            " when they do aggregations using cells of the same size to calculate statistics and visualize them or train "
            "prediction models. Others speed up queries by replacing geospatial lookups and joins "
            "with similar operations using IDs of H3 cells.")

st.markdown("However, even if you don't use spatial joins or don't have machine learning use cases, you likely "
            "store some geographic locations, typically as latitude and longitude pairs. The beauty of the H3 grid"
            " is in its simplicity and effectiveness in extracting value from such data. It offers an accessible way to start "
            "visualizing your geographic data, making it a versatile tool for many applications. Below are just a few use cases"
            " that show opportunities behind using H3 grid.")

st.subheader("Urban Mobility and Food Delivery")

st.markdown("These two industries are likely the most active in using the H3 grid. Sometimes H3 cell is used to calculate demand and supply in near-real time  "
            "and then predict those values for a future time interval. "
            "Since it's a fixed gread, meaning each Cell ID always points to the same geograpjical area it might be a valuable feature for ML models."
            " The most popular cell resolutions are probably 7 or 8 with the size of hexagons 0.5-5 sq.km."
            " Check below for the aggregated taxi pickup events of New York Taxi. To which area does this surge in Manhattan correspond?"
            " Is it Times Square?")

# ------ Visualisation 3 ---------
@st.cache_resource
def get_df_3(h3_resolut_3: int) -> pd.DataFrame:
    return session.sql(f'select h3_point_to_cell_string(pickup_location, {h3_resolut_3}) as h3, count(*) as count\n'\
                       'from snowpublic.streamlit.h3_ny_taxi_rides\n'\
                       'where 2 = 2\n'\
                       'group by 1\n').to_pandas()
@st.cache_resource
def get_quantiles_3(df_column: pd.Series, quantiles: List) -> pd.Series:
    return df_column.quantile(quantiles)

@st.cache_resource
def get_color_3(df_column: pd.Series, colors: List, vmin: int, vmax: int, index: pd.Series) -> pd.Series:
    color_map = cm.LinearColormap(colors, vmin=vmin, vmax=vmax, index=index)
    return df_column.apply(color_map.rgb_bytes_tuple)

@st.cache_resource
def get_layer_3(df: pd.DataFrame) -> pdk.Layer:
    return pdk.Layer("H3HexagonLayer", 
                     df, 
                     get_hexagon="H3",
                     get_fill_color="COLOR", 
                     get_line_color="COLOR",
                     get_elevation="COUNT/50000",
                     auto_highlight=True,
                     elevation_scale=50,
                     pickable=True,
                     elevation_range=[0, 300],
                     extruded=True,
                     coverage=1,
                     opacity=0.3)
   
col1, col2 = st.columns(2)
with col1:
    h3_resolut_3 = st.slider(
        "H3 resolution  ",
        min_value=6, max_value=9, value=7)

with col2:
    style_option_t_3 = st.selectbox("Style schema ",
                                ("Contrast", "Snowflake"), 
                                index=0)

df_3 = get_df_3(h3_resolut_3)

if style_option_t_3 == "Contrast":
    quantiles_3 = get_quantiles_3(df_3["COUNT"], [0, 0.25, 0.5, 0.75, 1])
    colors_3 = ['gray','blue','green','yellow','orange','red']
if style_option_t_3 == "Snowflake":
    quantiles_3 = get_quantiles_3(df_3["COUNT"], [0, 0.33, 0.66, 1])
    colors_3 = ['#666666', '#24BFF2', '#126481', '#D966FF']

df_3['COLOR'] = get_color_3(df_3['COUNT'], colors_3, quantiles_3.min(), quantiles_3.max(), quantiles_3)
layer_3 = get_layer_3(df_3)

st.image('https://sfquickstarts-obielov.s3.us-west-2.amazonaws.com/streamlit/gradient.png')
st.pydeck_chart(pdk.Deck(map_provider='carto',  map_style='light',
    initial_view_state=pdk.ViewState(
        latitude=40.74258515841464,
        longitude=-73.98452997207642, pitch=45, zoom=8),
        tooltip={
            'html': '<b>Pickups:</b> {COUNT}',
             'style': {
                 'color': 'white'
                 }
            },
    layers=[layer_3]))

# ------ Visualisation 3 End ---------

st.divider()
st.subheader("Telecommunication")

st.write(
  """Another industry that likes H3 is Telecommunication. They speed up queries by replacing geospatial lookups 
  and joins with similar operations using integer IDs of cells. For example when they calculate the mobile coverage of the road network. 
  See example in the [Geospatial Quickstart](https://quickstarts.snowflake.com/guide/geo_analysis_geometry/index.html?index=..%2F..index#6) (Step 7).
  Widget below visualizes 4G network coverage in Germany. Try different resolutions to see how the size of cells can impact the insights.
  """)


# ------ Visualisation 4 ---------
@st.cache_resource
def get_df_4(resolution: int) -> pd.DataFrame:
    return session.sql(f'select h3_latlng_to_cell_string(lat, lon, {resolution}) as h3, count(*) as count\n'\
                       'from snowpublic.streamlit.h3_celltowers\n'\
                       'group by 1 \n').to_pandas()

@st.cache_resource
def get_quantiles_4(df_column: pd.Series, quantiles: List) -> pd.Series:
    return df_column.quantile(quantiles)

@st.cache_resource
def get_color_4(df_column: pd.Series, colors: List, vmin: int, vmax: int, index: pd.Series) -> pd.Series:
    color_map = cm.LinearColormap(colors, vmin=vmin, vmax=vmax, index=index)
    return df_column.apply(color_map.rgb_bytes_tuple)

@st.cache_resource
def get_layer_4(df: pd.DataFrame) -> pdk.Layer:
    return pdk.Layer("H3HexagonLayer", 
                     df, 
                     get_hexagon="H3",
                     get_fill_color="COLOR", 
                     get_line_color="COLOR",
                     get_elevation="COUNT",
                     auto_highlight=True,
                     elevation_scale=50,
                     pickable=True,
                     elevation_range=[0, 3000],
                     extruded=False,
                     coverage=1,
                     opacity=0.5)
    
col1, col2 = st.columns(2)

with col1:
    h3_resolution_4 = st.slider("H3 resolution     ", min_value=2, max_value=7, value=2)

with col2:
    style_option_4 = st.selectbox("Style schema     ", ("Contrast", "Snowflake"), index=0)

df_4 = get_df_4(h3_resolution_4)

if style_option_4 == "Contrast":
    quantiles_4 = get_quantiles_4(df_4["COUNT"], [0, 0.25, 0.5, 0.75, 1])
    colors_4 = ['gray','blue','green','yellow','orange','red']
if style_option_4 == "Snowflake":
    quantiles_4 = get_quantiles_4(df_4["COUNT"], [0, 0.33, 0.66, 1])
    colors_4 = ['#666666', '#24BFF2', '#126481', '#D966FF']

df_4['COLOR'] = get_color_4(df_4['COUNT'], colors_4, quantiles_4.min(), quantiles_4.max(), quantiles_4)
layer_4 = get_layer_4(df_4)

st.image('https://sfquickstarts-obielov.s3.us-west-2.amazonaws.com/streamlit/gradient.png')
st.pydeck_chart(pdk.Deck(map_provider='carto', map_style='light',
    initial_view_state=pdk.ViewState(
        latitude=51.39817252610018,
        longitude=9.541183759445795, zoom=4),
                         tooltip={
        'html': '<b>Cell towers:</b> {COUNT}',
        'style': {
            'color': 'white'
        }
    },
    layers=[layer_4]))

# ------ Visualisation 6 End ---------
st.divider()
st.markdown("The world of geospatial data is vast and complex, but with tools like H3, it becomes more accessible and manageable. "
            "If you're a seasoned data analyst, a GIS professional the H3 functions can be a valuable addition to your toolkit."
            " If you just store latitude and longitude as separate columns and want to start capitalizing on your geospatial data, "
            "the H3 functions is probably the easiest to do so. We encourage you to try out these functions "
            "and discover the many ways they can enhance your spatial data processing and analysis. Happy mapping!")

col1, col2, col_3 = st.columns(3)
with col2:
    st.image('https://sfquickstarts-obielov.s3.us-west-2.amazonaws.com/streamlit/snowflake_h3.jpg', 
         width=173)
