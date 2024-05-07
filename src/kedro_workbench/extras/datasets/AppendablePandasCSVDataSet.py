# from kedro.extras.datasets.pandas import PandasCSVDataSet
from kedro.extras.datasets.pandas import CSVDataSet 
import pandas as pd

class AppendablePandasCSVDataSet(CSVDataSet):
    def __init__(self, filepath, load_args=None, save_args=None):
        super().__init__(filepath, load_args, save_args)

    def _load(self) -> pd.DataFrame:
        # Custom load behavior (if needed)
        # In this example, it's the same as the parent class
        return super()._load()

    def _save(self, data: pd.DataFrame):
        # Check if the file exists to determine if the header should be written
        file_exists = self._exists()

        # Append data to the existing CSV file
        data.to_csv(
            self._filepath, 
            mode='a', 
            header=not file_exists, 
            index=False, 
            **self._save_args
        )

    def _exists(self) -> bool:
        # Use the file system's exists method to check if the file exists
        return self._filesystem.exists(self._filepath)