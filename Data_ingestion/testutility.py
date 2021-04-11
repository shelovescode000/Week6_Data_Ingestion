import logging
import os
import subprocess
import yaml
import pandas as pd
import datetime 
import gc
import re
import csv


################
# File Reading #
################

def read_config_file(filepath):
    with open(filepath, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)


def replacer(string, char):
    pattern = char + '{2,}'
    string = re.sub(pattern, char, string) 
    return string

def col_header_val(df,table_config):
    '''
    replace whitespaces in the column
    and standardized column names
    '''
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('[^\w]','_',regex=True)
    df.columns = list(map(lambda x: x.strip('_'), list(df.columns)))
    df.columns = list(map(lambda x: replacer(x,'_'), list(df.columns)))
    expected_col = list(map(lambda x: x.lower(),  table_config['columns']))
    expected_col.sort()
    df.columns =list(map(lambda x: x.lower(), list(df.columns)))
    df = df.reindex(sorted(df.columns), axis=1)
    if len(df.columns) == len(expected_col) and list(expected_col)  == list(df.columns):
        print("column name and column length validation passed")
        return 1
    else:
        print("column name and column length validation failed")
        mismatched_columns_file = list(set(df.columns).difference(expected_col))
        print("Following File columns are not in the YAML file",mismatched_columns_file)
        missing_YAML_file = list(set(expected_col).difference(df.columns))
        print("Following YAML columns are not in the file uploaded",missing_YAML_file)
        logging.info(f'df columns: {df.columns}')
        logging.info(f'expected columns: {expected_col}')
        return 0

def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0
def file_size(file_path):
    """
    this function will return the file size
    """
    if os.path.isfile(file_path):
        file_info = os.stat(file_path)
        return convert_bytes(file_info.st_size)
    
def dataStats(df,table_config):
    n_col = "Total number of columns = {} columns\n".format(df.shape[1])
    n_row = "Total number of row = {} rows\n".format(df.shape[0])
    filesize = "file size {}\n".format(file_size(table_config))
    myText = open(r'File_summary.txt','w')
    myList = [n_col,n_row,filesize]
    for i in myList:
        myText.write(i)
    myText.close()
    
def outputfile(source_file,table_config):
    reader = csv.reader(open(source_file, "r"), delimiter=table_config['inbound_delimiter'])
    writer = csv.writer(open("output.txt", 'w'), delimiter=table_config['outbound_delimiter'])
    writer.writerows(reader)

    import gzip
    import shutil
    with open("output.txt", 'rb') as f_in, gzip.open('output.txt.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    
