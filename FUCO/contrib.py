import pathlib
import logging
import shutil
import os
import sys
import traceback
import json
import datetime
from datetime import date
from distutils.util import strtobool
from os.path import exists

# Today's date in YYYYMMDD format
TODAY = date.today().strftime("%Y%m%d")

def server_log(log_path):
    """
    Set up logging to both the console and a log file.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d %(message)s',
        datefmt='%Y%m%d %H:%M:%S')
    
    # StreamHandler for console logging
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    
    # FileHandler for file logging
    log_filename = datetime.datetime.now().strftime(log_path + '/%Y-%m-%d_rcv.log')
    fh = logging.FileHandler(log_filename)
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    
    # Adding handlers to the logger
    logger.addHandler(ch)
    logger.addHandler(fh)
    
    return logger

def conf_get(json_path: str):
    """
    Retrieve configuration from a JSON file.
    """
    if os.path.isfile(json_path):
        try:
            with open(json_path, encoding='utf-8-sig') as f:
                conf = json.loads(f.read())
                if isinstance(conf, dict):
                    return 0, conf
                else:
                    return 1, 'Conf data is not valid.'
        except Exception:
            return 1, exc_get()
    else:
        return 1, 'Cannot find conf data'

def exc_get():
    """
    Get the current exception traceback as a formatted string.
    """
    exc_type, exc_value, exc_traceback = sys.exc_info()
    error = traceback.format_exception(exc_type, exc_value, exc_traceback)
    error = (i.replace('\n','') for i in error)
    error = (i.replace('"','') for i in error)
    error = ''.join(i.replace("'", "") for i in error)
    return error

def files_match_get(path: str, today: bool):
    """
    Get a list of files in the specified directory.
    """
    folder_path = path + '/' + TODAY if today else path
    files_list = []
    
    if exists(folder_path):
        tmp_folder_path = folder_path.replace('/', '\\')
        if os.path.isdir(tmp_folder_path):
            dir_path = pathlib.Path(folder_path)
            files_list = [str(j).replace(tmp_folder_path, '').replace('\\', '') for j in dir_path.iterdir()]
    
    return folder_path, files_list

def folders_list_get(match_info: dict):
    """
    Get a list of folder paths based on the match information dictionary.
    """
    folders_path_list = [k + '/' + item for k, v in match_info.items() for item in v if v]
    return list(set(folders_path_list))

def cond_sort(cond: dict, key: str, num: int):
    """
    Increment a numerical value in a dictionary for a specific key.
    """
    if key in cond:
        num += 1
        cond[key] = num
    return cond

def file_trans(tmp_path, content, dst_data_transfer_path, file_name, dst_conn):
    """
    Transfer a file to a remote destination.
    """
    try:
        # Write content to a temporary file
        with open(tmp_path, 'w') as local_tr_file:
            local_tr_file.write(json.dumps(content))
        
        data_tr_path_split = dst_data_transfer_path.split('/')
        file_full_path = '%s/%s' % (''.join(data_tr_path_split[2:]), file_name)
        
        # Transfer the file
        with open(tmp_path, 'rb') as remote_tr_file:
            dst_conn.storeFile(data_tr_path_split[1], file_full_path, remote_tr_file, 60)
        
        # Remove the temporary file
        os.remove(tmp_path)
    except Exception as e:
        return 1, str(e)
    
    return 0, file_name

def xml_order_tag_get(filename: str, files_info: dict):
    """
    Get XML order tag based on filename and files information.
    """
    try:
        cond_string = '_'.join(list(files_info['feature'].values()))
        cond_string_split = cond_string.split('_')
        cond_info = cond_string_split[0], cond_string_split[1], cond_string_split[2]
    except Exception as e:
        return 1, str(e)
    
    return 0, filename, cond_info

def img_order_sort(cond_list: list):
    """
    Sort a list of conditions for image processing order.
    """
    try:
        # Sort the list based on multiple criteria
        cond_list.sort(key=lambda x: (x[1], x[2], x[0], x[3]))
        cond_sort_dict = {}
        
        # Enumerate and assign a sorting order
        for num, item in enumerate(cond_list, 1):
            str_item = '_'.join(item)
            cond_sort_dict[str_item] = num
    except Exception as e:
        return 1, str(e)
    
    return 0, cond_sort_dict
