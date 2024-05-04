import os
import osmnx as ox
import geopandas as gpd

class OSMPortDataDownloader:
    def __init__(self, crs_project, crs_global, country_code, output_path, geojson_gdf):
        self.crs_project = crs_project
        self.crs_global = crs_global
        self.osm_tags = {'landuse': ['harbour', 'industrial', 'port'], 'harbour': 'port'}
        ox.settings.log_console = True
        ox.settings.use_cache = True
        self.attributes = ['name', 'name:en', 'name_en']
        self.output_filename = f"{output_path}{country_code}/232_tran/{country_code}_tran_por_pt_s0_osm_pp_port.shp"
        self.geojson_gdf = geojson_gdf


    def download_and_process_data(self):
        geometry = self.geojson_gdf['geometry'].iloc[0]

        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")

        gdf = ox.geometries_from_polygon(geometry, tags=self.osm_tags)
        gdf = gdf.to_crs(epsg=self.crs_project)
        gdf['geometry'] = gdf.geometry.centroid
        gdf = gdf.to_crs(epsg=self.crs_global)

        # # Handle list-type fields before saving
        list_type_cols = [col for col, dtype in gdf.dtypes.items() if dtype == object]
        for col in list_type_cols:
            gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        # Make directories if they don't exist
        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)


        gdf['fclass'] = gdf['landuse']
        actual_tags = gdf.columns.intersection(self.attributes)
        missing_tags = set(self.attributes) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")
        
        collumns_to_keep = ['geometry', 'fclass'] + list(actual_tags) #+ list(self.osm_tags)
        gdf = gdf[collumns_to_keep]

        self.ensure_unique_column_names(gdf)

        # Save the data to a GeoPackage
        try:
            gdf.to_file(self.output_filename, driver='ESRI Shapefile')
        except Exception as e:
            print(f"An error occurred while saving the GeoDataFrame: {e}")

    def ensure_unique_column_names(self, gdf):
        new_columns = {}
        for col in gdf.columns:
            new_col = col[:10]
            counter = 1
            while new_col in new_columns.values():
                new_col = f"{col[:9]}{counter}"
                counter += 1
            new_columns[col] = new_col
        gdf.rename(columns=new_columns, inplace=True)
