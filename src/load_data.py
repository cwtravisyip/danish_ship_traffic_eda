import requests
import zipfile
import io 
import pandas as pd 
from typing import Generator, Union, Optional
import warnings


def load_zip_file(url:str,path: str = 'unzipped_files') -> None:
    """
    Unzip zip files from url and store in extract to specified directory.
    """
    response = requests.get(url)
    response.raise_for_status() 

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            
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
    
    
def load_csv_in_zip(url:str, chunksize: Optional[int] = 5000) -> Union[pd.DataFrame, Generator[pd.DataFrame, None, None]]:
        """
        Unzip the zipfile returned from a requests response and open the first csv file in the filename list passed in.
        Return a generator of chunks of pd.DataFrame for the data in the csv in the zipfile
        """
        response = requests.get(url)
        response.raise_for_status() 
        
        # extract the zip file into memory
        csvs_filename = find_csv(response)
        if len(csvs_filename) == 0:
            raise Exception("There are no csv in the zip file.")
        elif len(csvs_filename) > 1:
            warnings.warn(f"There are {len(csvs_filename)} csv file in the zip.")
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            # Load the first CSV file directly into a DataFrame
            df = pd.read_csv(zip_file.open(csvs_filename[0]),chunksize=chunksize)

        # assign attribute
        return df


if __name__ == '__main__':
    pass