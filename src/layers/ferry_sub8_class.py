import os
import osmnx as ox
import geopandas as gpd
import pandas as pd

class OSMFerryTerminalDataDownloader:
    def __init__(self, geojson_path, crs_project, crs_global, country_code, output_path):
        self.geojson_path = geojson_path
        self.crs_project = crs_project
        self.crs_global = crs_global
        self.osm_tags = {'amenity': 'ferry_terminal'}
        self.attributes = ['name', 'name:en', 'name_en']
        ox.config(log_console=True, use_cache=True)
        self.output_filename = f"{output_path}{country_code}/232_tran/{country_code}_tran_fte_pt_s2_osm_pp_ferryterminal.gpkg"

    def download_and_process_data(self):
        # Load the AOI from the GeoJSON file
        region_gdf = gpd.read_file(self.geojson_path)
        geometry = region_gdf['geometry'].iloc[0]
        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")

        # Download data from OSM based on the provided tags and the geometry of the AOI
        gdf = ox.geometries_from_polygon(geometry, tags=self.osm_tags)

        # Convert to the projected CRS to calculate centroids
        gdf_projected = gdf.to_crs(epsg=self.crs_project)
        gdf_projected['geometry'] = gdf_projected['geometry'].centroid

        # Convert back to the global CRS
        gdf = gdf_projected.to_crs(epsg=self.crs_global)

        # Handle list-type fields before saving
        list_type_cols = [col for col, dtype in gdf.dtypes.items() if dtype == object]
        for col in list_type_cols:
            gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        # Make directories if they don't exist
        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)

        actual_tags = gdf.columns.intersection(self.attributes)
        missing_tags = set(self.attributes) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")

         # Keep only the geometry, fclass, and the actual present tags
        columns_to_keep = ['geometry'] + list(actual_tags)   
        gdf = gdf[columns_to_keep]

        gdf = self.ensure_unique_column_names(gdf) 
        
    def ensure_unique_column_names(self, gdf):    
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

