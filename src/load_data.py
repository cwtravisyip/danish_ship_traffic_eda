import requests
import zipfile
import io 
import pandas as pd 
from typing import Generator
import warnings

def load_zip_file(url:str) -> requests.models.Response:
    """
    """
    response = requests.get(url)
    response.raise_for_status() 

    return response

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


if __name__ == '__main__':
    pass