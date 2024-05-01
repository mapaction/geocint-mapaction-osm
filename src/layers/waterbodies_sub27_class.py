import os
import osmnx as ox
import geopandas as gpd

class OSMLakeDataDownloader:
    def __init__(self, geojson_path, crs_project, crs_global, country_code):
        self.geojson_path = geojson_path
        self.output_filename = f"/home/gis/dedicated_disk/geocint/data/out/country_extractions/{country_code}/221_phys/{country_code}_phys_lak_py_s3_osm_pp_lake.shp"
        self.crs_project = crs_project
        self.crs_global = crs_global
        self.osm_tags = {'water': ['lake', 'reservoir']}
        self.attributes = ['name', 'name:en', 'name_en']
        ox.config(log_console=True, use_cache=True)

    def download_and_process_data(self):
        region_gdf = gpd.read_file(self.geojson_path)
        geometry = region_gdf['geometry'].iloc[0]

        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")

        gdf = ox.geometries_from_polygon(geometry, tags=self.osm_tags)
        
        # Filter for polygon geometries
        gdf_polygons = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]

        if self.crs_project:
            gdf_polygons = gdf_polygons.to_crs(epsg=self.crs_project)
        
        gdf_polygons = gdf_polygons.to_crs(epsg=self.crs_global)

        gdf_polygons = self.process_list_fields(gdf_polygons)
        gdf_polygons = self.ensure_unique_column_names(gdf_polygons)

        self.save_data(gdf_polygons)

    def process_list_fields(self, gdf):
        for col in gdf.columns:
            if gdf[col].dtype == object and gdf[col].apply(lambda x: isinstance(x, list)).any():
                gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        
        gdf['fclass'] = gdf['water']
        ##
        actual_tags = gdf.columns.intersection(self.attributes)
        missing_tags = set(self.attributes) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")

         # Keep only the geometry, fclass, and the actual present tags
        columns_to_keep = ['geometry','fclass'] + list(actual_tags)   
        gdf = gdf[columns_to_keep]
        ## 

        return gdf

    def ensure_unique_column_names(self, gdf):
        unique_columns = {}
        for col in gdf.columns:
            new_col = col[:10]
            counter = 1
            while new_col in unique_columns.values():
                new_col = f"{col[:9]}{counter}"
                counter += 1
            unique_columns[col] = new_col
        gdf.rename(columns=unique_columns, inplace=True)
        return gdf

    def save_data(self, gdf):
        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)
        try:
            gdf.to_file(self.output_filename, driver='ESRI Shapefile')
        except Exception as e:
            print(f"An error occurred while saving the GeoDataFrame: {e}")