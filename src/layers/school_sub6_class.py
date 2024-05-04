import os
import osmnx as ox
import geopandas as gpd
import pandas as pd

class OSMSchoolDataDownloader:
    osm_key = 'amenity'
    osm_value = 'school'
    additional_tags = [
        "operator", "operator_type", "capacity", "grades",
        "min_age", "max_age", "school:gender", 'name', 'name:en', 
        'name_en','operator_t','operator:t', 'school:gen', 'school_gen', 'osmid'
    ]

    def __init__(self, geojson_path, crs_project, crs_global, country_code, output_path):
        self.geojson_path = geojson_path
        self.crs_project = crs_project
        self.crs_global = crs_global
        ox.config(log_console=True, use_cache=True)
        self.output_filename = f"{output_path}{country_code}/210_educ/{country_code}_educ_edu_pt_s3_osm_pp_schools.gpkg"

    def download_and_process_data(self):
        region_gdf = gpd.read_file(self.geojson_path)
        geometry = region_gdf['geometry'].iloc[0]

        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")

        gdf = ox.geometries_from_polygon(geometry, tags={self.osm_key: self.osm_value})
        
        gdf_projected = gdf.to_crs(epsg=self.crs_project)
        gdf_projected['geometry'] = gdf_projected.geometry.centroid
        gdf = gdf_projected.to_crs(epsg=self.crs_global)

        # Check for 'fclass' column and add it if not present
        if 'fclass' not in gdf.columns:
            gdf['fclass'] = self.osm_value

        # Ensure unique column names for Shapefile format
        gdf = self.ensure_unique_column_names(gdf)

        # Identify the tags actually present in the data
        actual_tags = gdf.columns.intersection(self.additional_tags)
        
        # Add missing tags as new columns with missing values, if they are not present
        for tag in self.additional_tags:
            if tag not in actual_tags:
                gdf[tag] = pd.NA

        # Print a warning for any specified tags that are missing in the downloaded data
        missing_tags = set(self.additional_tags) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")

        # Convert list fields to string
        list_type_cols = [col for col, dtype in gdf.dtypes.items() if dtype == object]
        for col in list_type_cols:
            gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)

        # Keep only the geometry, fclass, and the actual present tags
        columns_to_keep = ['geometry', 'fclass'] + list(actual_tags)
        gdf = gdf[columns_to_keep]

        if not gdf.empty:
            gdf.to_file(self.output_filename, driver='GPKG')
        else:
            print("No data to save.")

    def ensure_unique_column_names(self, gdf):
        truncated_columns = {}
        final_columns = {}
        unique_suffixes = {}

        # Step 1: Truncate names
        for col in gdf.columns:
            truncated = col[:10]
            if truncated not in truncated_columns:
                truncated_columns[truncated] = 1
            else:
                truncated_columns[truncated] += 1
            final_columns[col] = truncated

        # Step 2: Resolve duplicates by adding a unique suffix
        for original, truncated in final_columns.items():
            if truncated_columns[truncated] > 1:
                if truncated not in unique_suffixes:
                    unique_suffixes[truncated] = 1
                else:
                    unique_suffixes[truncated] += 1
                suffix = unique_suffixes[truncated]
                suffix_length = len(str(suffix))
                truncated_with_suffix = truncated[:10-suffix_length] + str(suffix)
                final_columns[original] = truncated_with_suffix

        gdf.rename(columns=final_columns, inplace=True)
        return gdf
    
