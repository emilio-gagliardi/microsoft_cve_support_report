import os
import pandas as pd
from typing import Any, Dict
from kedro.io import AbstractDataSet
from pathlib import Path
import openpyxl
import re
# reading xlsx files requires openpyxl
class LatestExcelDataSet(AbstractDataSet):
    def __init__(self, filepath: str, load_args: Dict[str, Any] = None):
        """Initialize the dataset to load the latest Excel file based on its naming pattern."""
        super().__init__()
        self.filepath = Path(filepath)
        self.load_args = load_args if load_args is not None else {}

    def _load(self) -> pd.DataFrame:
        files = list(self.filepath.glob('*.xlsx'))
        if not files:
            raise FileNotFoundError(f"No Excel files found in directory {self.filepath}")
        latest_file = max(files, key=os.path.getctime)

        wb = openpyxl.load_workbook(latest_file)
        sheet = wb.active

        data = []
        urls = [[] for _ in range(sheet.max_row - 1)]
        hyperlink_columns = set()

        for i, row in enumerate(sheet.iter_rows(min_row=2, max_row=sheet.max_row)):
            row_data = []
            for j, cell in enumerate(row):
                row_data.append(cell.value)
                if cell.hyperlink:
                    urls[i].append(cell.hyperlink.target)
                    hyperlink_columns.add(j)
                elif j in hyperlink_columns:
                    urls[i].append(None)
            data.append(row_data)

        column_names = [cell.value for cell in sheet[1]]
        df = pd.DataFrame(data, columns=column_names)

        if hyperlink_columns:
            df_urls = pd.DataFrame(urls, columns=[f'{column_names[j]}_URL' for j in range(len(column_names)) if j in hyperlink_columns])
            df = pd.concat([df, df_urls], axis=1)
        else:
            print(f"Custom Dataset found no links in excel file")

        # Convert 'Release Date' string to datetime
        release_date_column_name = 'Release date'  # Adjust based on your actual column name
        df[release_date_column_name] = pd.to_datetime(df[release_date_column_name], errors='coerce', format='%b %d, %Y')

        # New logic to check 'kb_id' format and create custom 'kb_id' if necessary
        kb_id_column_name = 'Article'  # The column name for kb_id
        cve_id_column_name = 'Details'  # Adjust the column name as necessary
        build_number_column_name = 'Build Number'  # Assuming this is the name of the new column

        # Check if 'kb_id', 'cve_id', 'build_number', and 'Release Date' columns exist
        if kb_id_column_name in df.columns and cve_id_column_name in df.columns and build_number_column_name in df.columns and release_date_column_name in df.columns:
            df[kb_id_column_name] = df.apply(
                lambda row: f"{row[build_number_column_name]}_{row[cve_id_column_name]}_{row[release_date_column_name].strftime('%Y-%m-%d')}"
                if not re.match(r'^\d{7,}$', str(row[kb_id_column_name])) and row[cve_id_column_name] and pd.notnull(row[release_date_column_name])
                else row[kb_id_column_name],
                axis=1
            )
        else:
            print(f" something didn't work with these columns  {kb_id_column_name} {cve_id_column_name} {build_number_column_name} {release_date_column_name} compared to df.columns -> {df.columns}")
        
        return df

    def _save(self, data: pd.DataFrame) -> None:
        """Save method is not implemented as this dataset is read-only."""
        raise NotImplementedError("Save method of LatestExcelDataSet is not implemented.")

    def _describe(self) -> Dict[str, Any]:
        """Return a dict that describes the attributes of the dataset."""
        return dict(filepath=self.filepath, load_args=self.load_args)
