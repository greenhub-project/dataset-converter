# Dataset converter

> Dataset converter pipeline tool. Transforms dataset csv files into parquet files.

## Features

- [x] Simple automatic deployment
- [x] Extendable plugin system
- [x] Flexible configuration
- [x] Containerized setup

## Requirements

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Setup

- Copy `.env.example` to `.env` and fill the options

## Usage

- Download the .csv files to the `./data` folder
- Create the necessary config files. See [Configuration](#configuration) for more details.
- Run the application:
```shell
$ docker-compose up
```
- It will look for all `.yml`, for each dataset configured file, it will produce an optimized parquet file and a pickle file containing the pandas dtypes. The generated files are located in the `./data` folder.

### Output files
For each config file found, keeps the same file `name` as set in the config and create the following files:

#### ${name}.dytpes.pickle
Contains a dict python with the column:dtype for each entry.

#### ${name}.parquet.7z
Creates a parquet binary file compressed in 7z format from the dataframe processed.

## Plugins

A plugin system is available, where is possible to call additional procedures to modify the dataset files.

A plugin has a method named `apply` which receives a pandas Dataframe object and returns it at the end of the method. The plugin can be configured to run right after a file is loaded before the main processing is done or afterwards. 

The application includes the following packages:
- numpy
- pandas
- pyyaml
- pyarrow

Any extra dependencies can be added to the `requirements.txt` in the plugins folder, they will be installed on the startup of the application.

A sample plugin is provided as a template to get you started.

### Addons

A collection of ready-to use addons (configs and plugins) can be found [here](https://github.com/greenhub-project/dataset-converter-addons).

## Configuration

For each `.csv` file create `.yml` with the same name. A sample configuration file is provided.

### Arguments

#### name

Type: `string`

Table filename. This argument is required.

#### sep

Type: `string`

Default: `;`

Delimiter character to use. This argument is optional.

#### compression

Type: `boolean`

Default: `true`

Whether to compress the parquet files to `7z` format or not, when compression is on, it deletes the uncompressed parquet files afterwards. This argument is optional.

#### chunksize

Type: `number`

Chunk size, useful when files are large. This argument is optional. Ommiting this argument loads the whole file at once.

#### usecols

Type: `sequence`

List of columns to load from `.csv` file. This argument is optional. Ommiting this argument loads all columns from file.

#### parse_dates

Type: `sequence`

List of columns to parse with datetime format. This argument is optional.

#### plugins

Type: `sequence`

List of plugins to be applied to the dataframe object, plugins are called by the same order they are set in the configuration file. This argument is optional.

- **before:** Plugins called just after a file/chunk is loaded to memory.
- **after:** Plugins called at the end of downcasting process, before files are exported.
