import argparse
import os
import logging
import time
from multiprocessing import Pool, cpu_count

import geopandas as gpd

from layers.road_sub1_class import OSMRoadDataDownloader
from layers.railway_sub3_class import OSMRailwayDataDownloader
from layers.dam_sub5_class import OSMDamDataDownloader
from layers.school_sub6_class import OSMSchoolDataDownloader
from layers.uni_sub7_class import OSMEducationDataDownloader
from layers.ferry_sub8_class import OSMFerryTerminalDataDownloader
from layers.ferry_sub9_class import OSMFerryRouteDataDownloader
from layers.port_sub10_class import OSMPortDataDownloader
from layers.bank_sub11_class import OSMBankDataDownloader
from layers.atm_sub12_class import OSMATMDataDownloader
from layers.health_fac_sub13_class import OSMHealthDataDownloader
from layers.hosp_sub14_class import OSMHospitalDataDownloader
from layers.border_control_sub18_class import OSMBorderControlDataDownloader
from layers.settlement_sub19_class import OSMSettlementsDataDownloader
from layers.waterbodies_sub27_class import OSMLakeDataDownloader
from layers.large_river_sub28_class import OSMLargeRiverDataDownloader
from layers.phy_river_sub29_class import OSMRiverDataDownloader
from layers.canal_sub30_class import OSMCanalDataDownloader
from layers.rail2_sub31_class import OSMRailwayStationDataDownloader


# function 'get_crs_project' that takes a country code as input and returns the corresponding
# coordinate Reference System (CRS) code. CRS codes are used to translate between geographic locations
# and map coordinates.
def get_crs_project(country_code):
    crs_mapping = {
        'afg': 4255,
        'gha': 2136,
        'swe': 3006,
        'irq': 3893,
        'ken': 4210,
        # Add more mappings as necessary
    }
    return crs_mapping.get(country_code.lower(), 4326)


def download_and_process_worker(downloader):
    try:
        print(f"Running {str(downloader)} in a new process.")
        downloader.download_and_process_data()
        logging.info(f"Completed: {downloader.__class__.__name__}")
    except Exception as e:
        logging.error(f"Error in {downloader.__class__.__name__}: {e}")


# Define a function 'process_geojson_file' that takes the path of a geojson file as input.
def process_geojson_file(geojson_path: str, output_path: str, region_gdf):
    # Extract the country code from the filename of the geojson file. This assumes the file is named using the country code.
    country_code = os.path.basename(geojson_path).split('.')[0]
    # Call 'get_crs_project' function with the extracted country code to get the appropriate CRS code for the country.
    crs_project = get_crs_project(country_code)
    # Define a variable 'crs_global' with a value of 4326, representing the global CRS code (WGS 84).
    crs_global = 4326

    # Initialisee a list 'downloaders' with instances of various data downloader classes,
    # each initialised with parameters like the geojson path, country code, and CRS codes.
    # These instances are responsible for downloading and processing specific types of geographic data.
    downloaders = [
        OSMRoadDataDownloader(country_code, output_path, region_gdf),
        OSMRailwayDataDownloader(country_code, output_path, region_gdf),
        OSMDamDataDownloader(crs_project, crs_global, country_code,
                             output_path, region_gdf),
        OSMSchoolDataDownloader(crs_project, crs_global, country_code,
                                output_path, region_gdf),
        OSMEducationDataDownloader(crs_project, crs_global, country_code,
                                   output_path, region_gdf),
        OSMFerryTerminalDataDownloader(crs_project, crs_global,
                                       country_code, output_path, region_gdf),
        OSMFerryRouteDataDownloader(crs_project, crs_global,
                                    country_code, output_path, region_gdf),
        OSMPortDataDownloader(crs_project, crs_global, country_code,
                              output_path, region_gdf),
        OSMBankDataDownloader(crs_project, crs_global, country_code,
                              output_path, region_gdf),
        OSMATMDataDownloader(crs_project, crs_global, country_code,
                             output_path, region_gdf),
        OSMHealthDataDownloader(crs_project, crs_global, country_code,
                                output_path, region_gdf),
        OSMHospitalDataDownloader(crs_project, crs_global, country_code,
                                  output_path, region_gdf),
        OSMBorderControlDataDownloader(crs_project, crs_global,
                                       country_code, output_path, region_gdf),
        OSMSettlementsDataDownloader(crs_project, crs_global,
                                     country_code, output_path, region_gdf),
        OSMLakeDataDownloader(crs_project, crs_global, country_code,
                              output_path, region_gdf),
        OSMLargeRiverDataDownloader(crs_project, crs_global,
                                    country_code, output_path, region_gdf),
        OSMRiverDataDownloader(crs_project, crs_global, country_code,
                               output_path, region_gdf),
        OSMCanalDataDownloader(crs_project, crs_global, country_code,
                               output_path, region_gdf),
        OSMRailwayStationDataDownloader(crs_project, crs_global,
                                        country_code, output_path, region_gdf),
    ]

    # Run download and processing in parallel
    num_processes = max(cpu_count() - 2, 0)
    with Pool(processes=num_processes) as pool:
        # Map the download_and_process_worker function to each downloader instance
        pool.map(download_and_process_worker, downloaders)


# The 'main' function, which serves as the entry point for the script execution.
def main(geojson_dir: str, logs_dir: str, output_path: str):
    geojson_files = [os.path.join(geojson_dir, f) for f in os.listdir(geojson_dir) if
                     f.endswith(".json")]

    # log txt logic
    log_file = os.path.join(logs_dir, "logs", "processing_log.txt")
    # Ensures log directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Configures logging to write to a file and print to console
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[
                            logging.FileHandler(log_file, mode='a'),
                            logging.StreamHandler()
                        ])

    # Process each country (geojson) in order.
    for geojson_file in geojson_files:
        try:
            # Process each file using the process_geojson_file function.
            region_gdf = gpd.read_file(geojson_file)
            process_geojson_file(geojson_file, output_path, region_gdf)
            logging.info(f"Successfully processed {geojson_file}")
        except Exception as e:
            logging.error(f"Failed to process {geojson_file}: {e}")


if __name__ == "__main__":
    # Set up directory paths
    geojson_default_dir = "/home/gis/dedicated_disk/gis/geocint-mapaction/static_data/countries"
    log_default_dir = "/home/evangelos/data-pipeline/OSM-LAYERS"
    base_default_output_path = "/home/gis/dedicated_disk/geocint/data/out/country_extractions/"

    parser = argparse.ArgumentParser(description="Download and process OSM data")
    parser.add_argument('--geojson_dir', type=str, default=geojson_default_dir,
                        help='The directory holding the geojson files')
    parser.add_argument('--log_dir', type=str, default=log_default_dir,
                        help='The directory to write logs to')
    parser.add_argument('--output_path', type=str, default=base_default_output_path,
                        help='The directory to output the data to')
    args = parser.parse_args()

    # Start processing
    start = time.process_time()
    main(args.geojson_dir, args.log_dir, args.output_path)
    print(f"Finished osm processing in {time.process_time() - start} seconds.")
