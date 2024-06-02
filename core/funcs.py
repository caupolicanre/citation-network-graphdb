import json
import ijson
import chardet


def detect_encoding(file_path: str) -> str:
    '''
    Detect the encoding of a file.

    Parameters
    ----------
    file_path : str
        The path to the file.
    
    Returns
    -------
    str
        The encoding of the file.
    '''
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)

    result = chardet.detect(raw_data)
    return result['encoding']