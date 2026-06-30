"""BioGRID pipeline configuration."""

# Airflow connections
S3_CONN_ID = 'aws_s3'
DWH_CONN_ID = 'dwh_connection'

# Airflow variables
WEBSITE_URL_VARIABLE = 'biogrid_website_url'
RELEASES_URL_VARIABLE = 'biogrid_releases_url'

# Object storage (bronze)
BRONZE_BUCKET = 'bronze'
S3_KEY_TEMPLATE = 'biogrid/biogrid_{version}.tab3.zip'

# Warehouse (silver layer)
SILVER_SCHEMA = 'silver'
SILVER_TABLE = 'biogrid_data'

# Version resolution
DEFAULT_RELEASES_URL = 'https://downloads.thebiogrid.org/BioGRID/Release-Archive/'
VERSION_PATTERN = r'BIOGRID-(\d+\.\d+\.\d+)'
LATEST_VERSION = 'latest'

# Soda data-quality checks (data source name + checks file under soda/biogrid/)
SILVER_DATA_SOURCE = 'biogrid_silver'
GOLD_DATA_SOURCE = 'biogrid_gold'
SILVER_CHECKS_FILE = 'biogrid/checks_silver.yml'
GOLD_CHECKS_FILE = 'biogrid/checks_gold.yml'
