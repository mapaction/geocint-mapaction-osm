# OSM-LAYERS Project

The OSM-LAYERS project is a comprehensive Python-based solution for downloading, processing, and analyzing OpenStreetMap (OSM) data across multiple geographic entities. Utilizing the power of libraries such as OSMnx and  Geopandas this project aims to facilitate the extraction of meaningful geographic information structured into various categories including roads, rivers, educational institutions, and many more.

## Project Structure
The project is organized into modular Python scripts, each dedicated to handling a specific type of OSM data. This modular design ensures ease of management and scalability, accommodating a wide range of geographic data processing needs.

## Features

- Diverse Data Handling: Classes for downloading and processing data for roads, rivers, schools, hospitals, and other geographic features.
- Efficient Data Processing: Techniques for filtering, reprojecting, and formatting data to meet specific analysis or storage needs.
- CRS Handling: Functionality to map country codes to their corresponding Coordinate Reference Systems (CRS) for accurate geographic representation.

## Getting Started

### Prerequisites

Ensure you have Python 3.6+ installed on your system. The project depends on several Python libraries, including:

#### OSMnx
#### Geopandas
#### Pandas
#### Pathlib


#### All Python dependencies are installed by the geocint-runner/runner-install.sh


### Usage

Prepare your GeoJSON files: Place your GeoJSON files in the geocint-mapaction/static_data/countries/ directory. Ensure that each file is named according to the country code it represents.

Start the pipeline: Run the command bash geocint-runner/start_geocint.sh to start the whole data downloading and processing pipeline. The Makefile will be executed automatically.

Review the output: Processed data will be saved in the specified output directory, organized by data type and country code.

### Contributing

Contributions are what make the open-source community such an amazing place to learn, inspire, and create. Any contributions you make are greatly appreciated.

Fork the Project
Create your Feature Branch (git checkout -b feature/AmazingFeature)
Commit your Changes (git commit -m 'Add some AmazingFeature')
Push to the Branch (git push origin feature/AmazingFeature)
Open a Pull Request

### License

Distributed under the The MIT. See LICENSE for more information.

### Contact

ediakatos@mapaction.org

Project Link: https://github.com/yourrepository/OSM-LAYERS

