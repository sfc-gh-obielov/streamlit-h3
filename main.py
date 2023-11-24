import streamlit as st
import pandas as pd
import pydeck as pdk
import branca.colormap as cm
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import json

from PIL import Image

image = Image.open('./favicon.png')
         
st.set_page_config(
    page_title="H3: Simplifying the World's Map",
    page_icon=image,
)

session = Session.builder.configs(st.secrets["geodemo"]).create()
# st.set_page_config(page_title="H3 in Streamlit", layout="wide")

st.header("H3: Simplifying the World's Map", divider="rainbow")

st.markdown("<b>H3 Discrete Global Grid</b> is a way to divide the world into a grid of hexagonal cells, "
            "each with a unique identifier. It is a hierarchical grid, meaning that cells can be "
            "aggregated into larger cells, and vice versa. This makes it very efficient for processing geospatial data.", unsafe_allow_html=True)

st.markdown("Many companies use H3 grid today. Some of them use it for analytical and machine learning use cases"
            " when they do aggregations using cells of the same size to calculate statistics and visualize them or train "
            "prediction models. Others speed up queries by replacing geospatial lookups and joins "
            "with similar operations using IDs of H3 cells.")

st.markdown("However, even if you don't use spatial joins or don't have machine learning use cases, you likely "
            "store some geographic locations, typically as latitude and longitude pairs. The beauty of the H3 grid"
            " is in its simplicity and effectiveness in extracting value from such data. It offers an accessible way to start "
            "visualizing your geographic data, making it a versatile tool for many applications.")

st.markdown("To give you a practical example, below is a map showing the distribution of cell towers across the "
            "United States. This map is created using the "
            "<a href='https://app.snowflake.com/marketplace/listing/GZSVZ8ON6J/dataconsulting-pl-opencellid-open-database-of-cell-towers'>OpenCellID</a> dataset, "
            "from Snowflake Marketplace, which contains millions of data points. By applying the H3 grid system, we can organize these numerous"
            " points into hexagons of a chosen <a href='https://h3geo.org/docs/core-library/restable/'>resolution</a>. "
            "This approach not only simplifies the visualization but also makes it easier to "
            "understand and analyze the density and distribution of cell towers across the country.", unsafe_allow_html=True)

st.markdown("Try adjusting the resolution in the widget to see how changing the granularity of the data aggregation "
            "can lead to different observations and conclusions.", unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    h3_resolution = st.slider(
        "H3 resolution",
        min_value=1, max_value=6, value=3)

with col2:
    style_option = st.selectbox("Style schema",
                                ("Contrast", "Snowflake"), 
                                index=1)

df = session.sql(f'select h3_latlng_to_cell_string(lat, lon, {h3_resolution}) as h3, count(*) as count\n'\
'from OPENCELLID.PUBLIC.RAW_CELL_TOWERS\n'\
'where mcc between 310 and 316\n'\
'group by 1').to_pandas()

if style_option == "Contrast":
    quantiles = df["COUNT"].quantile([0, 0.25, 0.5, 0.75, 1])
    colors = ['gray','blue','green','yellow','orange','red']
if style_option == "Snowflake":
    quantiles = df["COUNT"].quantile([0, 0.33, 0.66, 1])
    colors = ['#666666', '#24BFF2', '#126481', '#D966FF']

color_map = cm.LinearColormap(colors, vmin=quantiles.min(), vmax=quantiles.max(), index=quantiles)
df['COLOR'] = df['COUNT'].apply(color_map.rgb_bytes_tuple)
st.pydeck_chart(pdk.Deck(map_style=None,
    initial_view_state=pdk.ViewState(
        latitude=38.51405689475766,
        longitude=-94.50284957885742, zoom=3),
    layers=[pdk.Layer("H3HexagonLayer", df, get_hexagon="H3",
                      get_fill_color="COLOR", 
                      get_line_color="COLOR",
                      opacity=0.5, extruded=False)]))


st.markdown("Let's dive deeper into specific functions of the H3 library that are particularly"
            " useful for spatial data processing.", unsafe_allow_html=True)
st.markdown("<h3>H3_COVERAGE and H3_POLYGON_TO_CELLS</h3>"
            "The <a href='https://docs.snowflake.com/en/sql-reference/functions/h3_coverage'>H3_COVERAGE</a></h3> function provides full coverage of a polygon with H3 cells, meaning that "
            "it returns all the H3 cells that intersect with the given polygon. "
            "This is particularly useful when you need a comprehensive representation of a spatial area. "
            "Higher resolutions lead to smaller cells, offering a more detailed coverage. Additionaly, "
            "just like Snowflake's <a href='https://docs.snowflake.com/en/sql-reference/data-types-geospatial#label-data-types-geography'>GEOGRAPHY</a>"
            " data type, H3_COVERAGE utilizes spherical geometry for its calculations. "
            "It means it takes into account the curvature of the Earth when determining which H3 cells intersect with a given polygon."
            " This is especialy important when you work with spatial objects on the global scale, e.g tesselate shapes of countries.", unsafe_allow_html=True)
st.markdown("<br>In contrast, the <a href='https://docs.snowflake.com/en/sql-reference/functions/h3_polygon_to_cells'>H3_POLYGON_TO_CELLS</a>"
            " function is centroid-based. It returns the H3 cells whose centroids are within the specified polygon. "
            "And unlike H3_COVERAGE it operates on planar geometry."
            " It assumes a flat surface for its calculations, which simplifies the processing but can introduce distortions, especially over large areas.", unsafe_allow_html=True)
st.markdown("Explore the differences between these functions at both global and local scales using the provided widget. "
            "Compare the results against the initial <span style='color:rgba(217, 102, 255, 1);'>Light Purple</span> polygon to understand their behavior.", unsafe_allow_html=True)


col1, col2, col3 = st.columns(3)

with col1:
    poly_scale = st.selectbox("Scale of polygon", ("Global", "Local"), index=0)
    if poly_scale == 'Global':
        min_v, max_v, v, z, lon, lat = 2, 5, 4, 3, -94.50284957885742, 38.51405689475766
    else:
        min_v, max_v, v, z, lon, lat = 7, 10, 9, 10, -73.98452997207642, 40.74258515841464

with col2:
    original_shape = st.selectbox("Show original shape", ("Yes", "No"),  index=0)

with col3:
    h3_res = st.slider( "Resolution", min_value=min_v, max_value=max_v, value=v)

df_shape = session.sql(f"select geog from streamlit.viz.polygon_spherical where scale_of_polygon = '{poly_scale}'").to_pandas()

df_shape["coordinates"] = df_shape["GEOG"].apply(lambda row: json.loads(row)["coordinates"][0])
layer_shape = pdk.Layer("PolygonLayer", df_shape, opacity=0.9, stroked=True, get_polygon="coordinates",
                        filled=False, extruded=False, wireframe=True, get_line_color=[217, 102, 255],line_width_min_pixels=1)

df_coverage = session.sql(f"select value::string as h3 from streamlit.viz.polygon_planar, TABLE(FLATTEN(h3_coverage_strings(geog, {h3_res}))) where scale_of_polygon = '{poly_scale}'").to_pandas()
layer_coverage = pdk.Layer("H3HexagonLayer", df_coverage, get_hexagon="H3", extruded=False, 
                           stroked=True, filled=False, get_line_color=[18, 100, 129], line_width_min_pixels=1)

df_polyfill = session.sql(f"select value::string as h3 from streamlit.viz.polygon_planar, TABLE(FLATTEN(h3_polygon_to_cells_strings(geog, {h3_res}))) where scale_of_polygon = '{poly_scale}'").to_pandas()
layer_polyfill = pdk.Layer("H3HexagonLayer", df_polyfill, get_hexagon="H3", extruded=False, 
                      stroked=True, filled=False, get_line_color=[36, 191, 242], line_width_min_pixels=1)

if original_shape == "Yes":
    visible_layers_coverage = [layer_coverage, layer_shape]
    visible_layers_polyfill = [layer_polyfill, layer_shape]
else:
    visible_layers_coverage = [layer_coverage]
    visible_layers_polyfill = [layer_polyfill]

st.markdown('<b>H3_COVERAGE</b>', unsafe_allow_html=True)
st.pydeck_chart(pdk.Deck(map_style=None,
    initial_view_state=pdk.ViewState(
        latitude=lat,
        longitude=lon, zoom=z),
    layers=visible_layers_coverage))

st.markdown('<b>H3_POLYGON_TO_CELLS</b>', unsafe_allow_html=True)
st.pydeck_chart(pdk.Deck(map_style=None,
    initial_view_state=pdk.ViewState(
        latitude=lat,
        longitude=lon, zoom=z),
    layers=visible_layers_polyfill))

st.markdown("The world of geospatial data is vast and complex, but with tools like H3, it becomes more accessible and manageable. "
            "Whether you're a seasoned data analyst, a GIS professional, or just starting out in the realm of geospatial data, "
            "the H3 functions can be a valuable addition to your toolkit. So, we encourage you to try out these functions in Snowflake "
            "and discover the many ways they can enhance your spatial data processing and analysis. Happy mapping!")
st.write("---")
st.markdown("<a href='https://docs.snowflake.com/en/sql-reference/data-types-geospatial#label-data-types-geospatial-h3'>Snowflake H3 Documentation</a><br>"
            "<a href='https://github.com/sfc-gh-obielov/streamlit-h3/'>GitHub of this page</a>", unsafe_allow_html=True)