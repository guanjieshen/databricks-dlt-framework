import base64
import os
from dotenv import load_dotenv, find_dotenv


AUTOLOADER_SCHEMA_ROOT_PATH = "/Volumes/gshen_etl_framework_dev/autoloader/schemas"

def generated_autoloader_schema_path(source_table_path:str):
    """
    Generates a unique location within ADLS to store the autoloader schema path for the asset.
    This should be deterministic based on the asset's name.

    This uses the root path specific in the .env file.

    Returns:
        str: Cloud storage location for the checkpoint path.
    """

    
    string = "source_table_path"
    string_bytes = string.encode("ascii") 
      
    base64_bytes = base64.b64encode(string_bytes) 
    base64_string = base64_bytes.decode("ascii")
    path = AUTOLOADER_SCHEMA_ROOT_PATH  + '/' + base64_string
    return path

