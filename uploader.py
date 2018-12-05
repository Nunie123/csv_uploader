'''
example usage:
`source activate uploader`
`python uploader.py -f test.csv -c vwtest -s utils -t foo`
'''

import argparse, os
from porthole.connections import ConnectionManager
import pandas as pd

def read_source_data(source_file):
    filename, file_extension = os.path.splitext(source_file)
    if file_extension in ['xls', 'xlsx']:
        df = pd.read_excel(io=source_file)
        df.index += 1
    elif file_extension == '.csv':
        df = pd.read_csv(filepath_or_buffer=source_file)
    return df

def get_filename_without_extension(filepath):
    basename = os.path.basename(filepath)
    name_without_extension = os.path.splitext(basename)[0]
    return name_without_extension


def upload(source_file, connection, schema, table = None, insert_into_existing_table=False):
    if_exists = 'append' if insert_into_existing_table else 'replace'
    table_name = table if table else get_filename_without_extension(source_file)
    df = read_source_data(source_file)
    with ConnectionManager(connection) as conn:
        df.to_sql(
            name=table_name,
            con=conn.engine,
            if_exists=if_exists,
            schema=schema,
            index=False
        )
        conn.commit()
    

def get_parser():
    parser = argparse.ArgumentParser(description="Simple-DB-Builder")
    parser.add_argument("-f", "--file_name", type=str, help="Provide name of source file.")
    parser.add_argument("-c", "--connection_name", type=str, help="Provide name of target connection.")
    parser.add_argument("-s", "--schema_name", type=str, help="Provide name of target schema.")
    parser.add_argument("-t", "--table_name", type=str, help="Provide name of target table. Filename will be used if not provided.")
    parser.add_argument("-i", "--insert_into_existing_table", action='store_true', help="Flag indicates if data should be loaded into existing table.")
    return parser


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    upload(source_file=args.file_name, 
            connection=args.connection_name, 
            schema=args.schema_name, 
            table=args.table_name, 
            insert_into_existing_table=args.insert_into_existing_table)