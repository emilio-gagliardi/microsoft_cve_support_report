# src/your_project_name/io/safe_partitioned_dataset.py
from kedro.io import PartitionedDataSet
from kedro.io.core import DataSetError
import logging

logger = logging.getLogger(__name__)
class SafePartitionedDataSet(PartitionedDataSet):
    def _load(self):
        print("CUSTOM PARTITIONED DATSET _LOAD")
        try:
            loaded_partitions = super()._load()
            if not loaded_partitions:
                # Handle empty partition case
                logging.warning("No partitions found. Returning empty dict.")
                return {}
            return loaded_partitions
        except DataSetError as e:
            logging.warning(f"Error loading partitions: {e}")
            # Handle the error as needed, for example, return an empty dict
            return {}
