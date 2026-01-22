from abc import ABC, abstractmethod
import os
import zipfile
import tempfile
import pandas as pd


class DataIngestor(ABC):
    @abstractmethod
    def ingest(self, source: str) -> pd.DataFrame:
        pass


class ZipDataIngestor(DataIngestor):
    def ingest(self, source: str) -> pd.DataFrame:
        if not os.path.exists(source):
            raise FileNotFoundError(f"File not found: {source}")

        if not source.endswith(".zip"):
            raise ValueError("Source must be a .zip file")

        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(source, "r") as zip_ref:
                zip_ref.extractall(temp_dir)

            csv_files = [f for f in os.listdir(temp_dir) if f.endswith(".csv")]

            if len(csv_files) == 0:
                raise FileNotFoundError("No CSV file found in the ZIP archive")
            if len(csv_files) > 1:
                raise ValueError("Multiple CSV files found in the ZIP archive")

            csv_path = os.path.join(temp_dir, csv_files[0])
            df = pd.read_csv(csv_path)

        return df
        

# Extract the file archive and read the CSV file into a DataFrame

if __name__ == "__main__":
    ingestor = ZipDataIngestor()
    df = ingestor.ingest("archive (10).zip")
    print(df.head())

