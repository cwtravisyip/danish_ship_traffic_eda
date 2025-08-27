import requests
import zipfile
import io 
import pandas as pd 
from typing import Generator
import warnings


def load_zip_file(url:str,path: str = 'unzipped_files') -> None:
    """
    the zip files for the historical AIS can be found here: http://web.ais.dk/aisdata/
    """
    response = requests.get(url)
    response.raise_for_status() 

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
    # List the contents of the ZIP file
        print("Contents of the ZIP file:")
        for file_name in zip_file.namelist():
            print(file_name)
            
        # Optionally extract files to a folder
        zip_file.extractall(path)
        print(f"Files extracted to {path} folder.")

    return None

def find_csv(res: requests.models.Response) -> list:
    """
    Open the zipfile returned from a request response and return the filenames in a list
    """
    # Open the ZIP file in memory
    with zipfile.ZipFile(io.BytesIO(res.content)) as zip_file:
        # Find the CSV file within the ZIP
        csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
        
        if not csv_files:
            raise ValueError("No CSV file found in the ZIP archive.")
        elif len(csv_files) > 1:
            warnings.warn(f"There exists {len(csv_files)} csv file in the zipfile.")
    
    return csv_files
    
def load_csv_in_zip(filename:list,res: requests.models.Response, chunksize: int = 5000) \
    -> Generator[pd.DataFrame, None, None]:
    """
    Unzip the zipfile returned from a requests response and open the first csv file in the filename list passed in.
    Return a generator of chunks of pd.DataFrame for the data in the csv in the zipfile
    """
    # Open the ZIP file in memory
    with zipfile.ZipFile(io.BytesIO(res.content)) as zip_file:
        if len(filename)>1:
            warnings.warn(f"There exists {len(filename)} csv file in the zipfile.")
            warnings.warn(f"Unpacking the first csv file {filename[0]}.")
        elif not filename:
            raise ValueError("No CSV file specified.")
        else:
            pass

            # Load the first CSV file directly into a DataFrame
        return pd.read_csv(zip_file.open(filename[0]), chunksize = chunksize)
    
class url_zipfile_loader:
    def __init__(self, url):
        self.url = url

    def __enter__(self):
        response = requests.get(self.url)
        response.raise_for_status() 
            # Open the ZIP file in memory
        # with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        #     # Find the CSV file within the ZIP
        #     csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
            
        #     if not csv_files:
        #         raise ValueError("No CSV file found in the ZIP archive.")
            
        #     # Load the first CSV file directly into a DataFrame
        #     self.csv_fie = 
        #     return zip_file.open(csv_files[0])


    def __exit__(self, exc_type, exc_val, exc_tb):
        print("exit")


if __name__ == '__main__':
    pass