import os
import osmnx as ox
import geopandas as gpd
import pandas as pd

class OSMFerryRouteDataDownloader:
    def __init__(self, geojson_path, crs_project, crs_global, country_code, output_path):
        self.geojson_path = geojson_path
        self.crs_project = crs_project
        self.crs_global = crs_global
        # OSM tags to search for ferry routes
        self.osm_tags = {'route': 'ferry'}  
        ox.config(log_console=True, use_cache=True)
        self.attributes = ['name', 'name:en', 'name_en']
        self.output_filename = f"{output_path}{country_code}/232_tran/{country_code}_tran_fer_ln_s2_osm_pp_ferryroute.gpkg"
    
    def download_and_process_data(self):
        # Load the Area of Interest (AOI) from the GeoJSON file
        region_gdf = gpd.read_file(self.geojson_path)
        geometry = region_gdf['geometry'].iloc[0]

        # Check if the geometry is a Polygon or MultiPolygon
        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")

        # Download data from OSM based on the provided tags and the geometry of the AOI
        gdf = ox.geometries_from_polygon(geometry, tags=self.osm_tags)

        # Ensure all tags are represented as columns, even if no data is present
        for key in self.osm_tags.values():
            if key not in gdf.columns:
                gdf[key] = pd.NA

        list_type_cols = [col for col, dtype in gdf.dtypes.items() if dtype == object]
        for col in list_type_cols:
            gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        # Identify actual and missing tags
        actual_tags = gdf.columns.intersection(self.attributes)
        missing_tags = set(self.attributes) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")

        # Define columns to keep, excluding unwanted fields
        unwanted_fields = {'manmade', 'image', 'ref', 'source', 'layer', 'surface'}
        columns_to_keep = set(['geometry'] + list(actual_tags)) - unwanted_fields
        columns_to_keep = list(columns_to_keep)  # Convert back to list if necessary for further operations
        gdf = gdf[columns_to_keep]


        # Make directories if they don't exist
        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)

        # Truncate column names and ensure uniqueness
        unique_columns = {}
        for col in gdf.columns:
            col_truncated = col[:10]
            if col_truncated in unique_columns:
                unique_columns[col_truncated] += 1
                col_truncated = f"{col_truncated}_{unique_columns[col_truncated]}"
            else:
                unique_columns[col_truncated] = 1
            gdf.rename(columns={col: col_truncated}, inplace=True)

        # Save the data to a GeoPackage
        if not gdf.empty:
            gdf.to_file(self.output_filename, driver='GPKG')
        else:
            print("No data to save.")
