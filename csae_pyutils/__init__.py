import os
import json

import requests
import pandas as pd
from io import BytesIO
import owncloud


def load_json(file_path: str, encoding="utf-8") -> dict:
    """Load and parse JSON file.
    Args:
        file_path (str): Path to the JSON file to be loaded.
        encoding (str, optional): Encoding used to open the file. Defaults to "utf-8".
    Returns:
        dict: Dictionary containing the parsed JSON data.
    Raises:
        FileNotFoundError: If the specified file does not exist.
        JSONDecodeError: If the file contains invalid JSON.
    """

    with open(file_path, "r", encoding=encoding) as fp:
        data = json.load(fp)
    return data


def gsheet_to_df(sheet_id: str) -> pd.DataFrame:
    """Converts a Google Sheet to a pandas DataFrame.
    This function takes a Google Sheet ID and returns the sheet's content as a pandas DataFrame.
    The sheet must be publicly accessible or have appropriate sharing settings.
    Args:
        sheet_id (str): The ID of the Google Sheet. This can be found in the sheet's URL between
            /d/ and /edit.
    Returns:
        pd.DataFrame: A pandas DataFrame containing the sheet's data.
    Raises:
        requests.exceptions.RequestException: If there's an error fetching the sheet.
        pd.errors.EmptyDataError: If the sheet is empty or contains no valid data.
    Example:
        >>> sheet_id = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
        >>> df = gsheet_to_df(sheet_id)
    """

    GDRIVE_BASE_URL = "https://docs.google.com/spreadsheet/ccc?key="
    url = f"{GDRIVE_BASE_URL}{sheet_id}&output=csv"
    r = requests.get(url)
    print(r.status_code)
    data = r.content
    df = pd.read_csv(BytesIO(data))
    return df


def upload_files_to_owncloud(file_list: list, user: str, pw: str, folder="pfp-data"):
    """Uploads files to OEAW ownCloud instance.
    This function uploads a list of files to a specified folder in the OEAW ownCloud instance.
    It creates the destination folder if it doesn't exist.
    Args:
        file_list (list): List of file paths to upload
        user (str): ownCloud username
        pw (str): ownCloud password
        folder (str, optional): Destination folder name in ownCloud. Defaults to "pfp-data"
    Returns:
        result: Response from the last file upload operation
    Raises:
        Various ownCloud exceptions may be raised if connection or upload fails
    Example:
        >>> files = ['/path/to/file1.txt', '/path/to/file2.pdf']
        >>> result = upload_files_to_owncloud(
        ...     file_list=files,
        ...     user='your_username',
        ...     pw='your_password',
        ...     folder='my-uploads'
        ... )
    """

    collection = folder
    oc = owncloud.Client("https://oeawcloud.oeaw.ac.at")
    oc.login(user, pw)

    try:
        oc.mkdir(collection)
    except:  # noqa: E722
        pass

    files = file_list
    for x in files:
        _, tail = os.path.split(x)
        owncloud_name = f"{collection}/{tail}"
        print(f"uploading {tail} to {owncloud_name}")
        result = oc.put_file(owncloud_name, x)

    return result
