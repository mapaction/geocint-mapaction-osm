import os
import osmnx as ox
import geopandas as gpd
import pandas as pd

class OSMRiverDataDownloader:
    def __init__(self, crs_project, crs_global, country_code, output_path, geojson_gdf):
        self.output_filename = f"{output_path}{country_code}/221_phys/{country_code}_phys_riv_ln_s3_osm_pp_rivers.shp"
        self.crs_project = crs_project
        self.crs_global = crs_global
        self.osm_key = 'waterway'
        self.osm_value = 'river'
        self.exclude_values = ['stream', 'canal', 'ditch', 'drain']
        self.attributes = ['name', 'name:en', 'name_en']
        ox.config(log_console=True, use_cache=True)
        self.geojson_gdf = geojson_gdf

    def download_and_process_data(self):
        # Load the region of interest geometry
        geometry = self.geojson_gdf['geometry'].iloc[0]

        # Ensure the geometry is a polygon
        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")

        # Download OSM data
        gdf = ox.geometries_from_polygon(geometry, {self.osm_key: self.osm_value})
        gdf = gdf[~gdf[self.osm_key].isin(self.exclude_values)]
        gdf = gdf[gdf.geometry.type == 'LineString']

        # Reproject geometries
        gdf_projected = gdf.to_crs(epsg=self.crs_project)
        gdf_projected = gdf_projected.to_crs(epsg=self.crs_global)

        # Handle list-type fields
        gdf_projected = self.process_list_fields(gdf_projected)

        # Ensure unique column names
        gdf_projected = self.ensure_unique_column_names(gdf_projected)

        # Save the GeoDataFrame
        self.save_data(gdf_projected)

    def process_list_fields(self, gdf):
        for col in gdf.columns:
            if isinstance(gdf[col].iloc[0], list):
                gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if x else '')
        
        gdf['fclass'] = self.osm_value
        ##
        actual_tags = gdf.columns.intersection(self.attributes)
        missing_tags = set(self.attributes) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")

         # Keep only the geometry, fclass, and the actual present tags
        columns_to_keep = ['geometry','fclass'] + list(actual_tags)   
        gdf = gdf[columns_to_keep]

        return gdf

    def ensure_unique_column_names(self, gdf):
        unique_columns = {}
        for col in gdf.columns:
            col_truncated = col[:10]
            counter = 1
            while col_truncated in unique_columns.values():
                col_truncated = f"{col[:9]}{counter}"
                counter += 1
            unique_columns[col] = col_truncated
        gdf.rename(columns=unique_columns, inplace=True)
        return gdf

    def save_data(self, gdf):
        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)
        try:
            gdf.to_file(self.output_filename, driver='ESRI Shapefile')
        except Exception as e:
            print(f"An error occurred while saving the GeoDataFrame: {e}")