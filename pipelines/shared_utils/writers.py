from pyspark.sql import DataFrame
import os
from dotenv import load_dotenv
from enum import Enum

# Load the root path for the checkpoint and autoloader schema
load_dotenv()
STREAM_CHECKPOINT_ROOT_PATH = os.getenv("STREAM_CHECKPOINT_ROOT_PATH")
AUTOLOADER_SCHEMA_ROOT_PATH = os.getenv("AUTOLOADER_SCHEMA_ROOT_PATH")

# Define Valid Config Values
class WriteMode(Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    SCD1 = "scd1"
    SCD2 = "scd2"


class RefreshMode(Enum):
    FULL = "full"
    INCREMENTAL = "incremental"

class TriggerMode(Enum):
    TRIGGERED = "triggered"
    CONTINOUS = "continous"

class AutoloaderWriter:
    def __init__(
        self,
        df: DataFrame,
        cluster_by_cols: list = None,
        uc_catalog: str = None,
        uc_schema: str = None,
        uc_table: str = None,
    ):
        self.df = df
        cluster_by_cols = cluster_by_cols or []
        self.uc_catalog = uc_catalog
        self.uc_schema = uc_schema
        self.uc_table = uc_table

    def generate_checkpoint_path(self):
        """
        Generates a unique location within ADLS to store the streaming checkpoint for the asset.
        This should be deterministic based on the asset's name.

        This uses the root path specific in the .env file.

        Returns:
            str: Cloud storage location for the checkpoint path.
        """
        # TODO: Implement this function

    def generated_autoloader_schema_path(self):
        """
        Generates a unique location within ADLS to store the autoloader schema path for the asset.
        This should be deterministic based on the asset's name.

        This uses the root path specific in the .env file.

        Returns:
            str: Cloud storage location for the checkpoint path.
        """
        # TODO: Implement this function


    def write_datalake_only(
        data_lake_location: str,
        write_mode: WriteMode = WriteMode.APPEND,
        refresh_mode: RefreshMode = RefreshMode.INCREMENTAL,
        trigger_mode: TriggerMode = TriggerMode.TRIGGERED
    ):
        """
        Writes the dataframe to the datalake location specified.
        """
        # TODO: Implement this function

    def write_uc_external_table(
        data_lake_location: str,
        uc_catalog: str,
        uc_schema: str,
        uc_table: str,
        write_mode: WriteMode = WriteMode.APPEND,
        refresh_mode: RefreshMode = RefreshMode.INCREMENTAL,
        trigger_mode: TriggerMode = TriggerMode.TRIGGERED
    ):
        """
        Writes the dataframe to an External Table in the Unity Catalog
        """
        # TODO: Implement this function

    def write_uc_managed_table(
        uc_catalog: str,
        uc_schema: str,
        uc_table: str,
        write_mode: WriteMode = WriteMode.APPEND,
        refresh_mode: RefreshMode = RefreshMode.INCREMENTAL,
        trigger_mode: TriggerMode = TriggerMode.TRIGGERED
    ):
        """
        Writes the dataframe to an Managed Table in the Unity Catalog
        """
        # TODO: Implement this function
