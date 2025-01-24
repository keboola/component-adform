# Adform Metadata Extractor

A Keboola Connection component for extracting data from Adform masterdata service.

## Prerequisites

- Agency account with permission to access Adform External API (https://api.adform.com/)
- Master Data service enabled
- Valid credentials provided by Adform after contract signing

## Authorization

The component uses OAuth 2.0 for authentication. Authorize your account by clicking the "AUTHORIZE ACCOUNT" button in the authorization section of the component. Continue with the authorization process in the Adform masterdata service.

## Configuration Parameters

The component accepts the following configuration parameters:

### Source Configuration

- `setup_id` (required): Your unique Master Data setup identifier provided by Adform
- `days_interval` (required): Number of days to look back for data retrieval
- `hours_interval` (required): Number of hours to look back for data retrieval
- `date_to` (optional): Upper boundary of the time interval for data retrieval
  - Format: "YYYY-MM-DD HH:MM" or relative 1 day ago, 1 week ago
  - If not specified, current time is used
- `datasets` (optional): List of datasets to retrieve
  - Default: ["Click", "Impression", "Trackingpoint", "Event"]
- `file_charset` (optional): Character encoding of the returned dataset
  - Default: "UTF-8"
- `meta_files` (optional): List of metadata files to retrieve
  - Example: ["geolocations", "campaigns"]

### Destination Configuration

- `table_name` (optional): Name of the destination table
- `load_type` (optional): Type of data load
  - Options: "full_load" or "incremental_load"
  - Default: "incremental_load"
- `override_pkey` (optional): List of primary key overrides for specific datasets
  - Format: 
    ```json
    {
      "dataset": "dataset_name",
      "pkey": ["column1", "column2"]
    }
    ```

### Debug Mode

- `debug` (optional): Enable debug mode
  - Default: false
  - Set to true for additional logging

## Example Configuration

```json
{
  "source": {
    "setup_id": "your-setup-id",
    "days_interval": 7,
    "hours_interval": 24,
    "datasets": ["Click", "Impression"],
    "meta_files": ["geolocations", "campaigns"],
    "file_charset": "UTF-8"
  },
  "destination": {
    "table_name": "adform_data",
    "load_type": "incremental_load",
    "override_pkey": [
      {
        "dataset": "Click",
        "pkey": ["id", "timestamp"]
      }
    ]
  },
  "debug": false
}
```

## Important Notes

1. For incremental loads, ensure primary keys are properly set in the KBC Storage UI after the first successful import.
2. Metadata tables are always imported in full_load mode, overwriting previous data.
3. The time interval for data retrieval is calculated based on both `days_interval` and `hours_interval`.


Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following
command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/keboola/component-adform adform_metadata_extractor
cd adform_metadata_extractor
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers
documentation](https://developers.keboola.com/extend/component/deployment/)
