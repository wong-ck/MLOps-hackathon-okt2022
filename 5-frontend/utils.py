import argparse
from google.oauth2 import service_account
import pandas_gbq
import numpy as np


def load_data(service_account_file,project_id,query):
    credentials = service_account.Credentials.from_service_account_file(service_account_file)
    result_dataframe = pandas_gbq.read_gbq(query, project_id=project_id, credentials=credentials)
    return result_dataframe


def process_data(df):
    df['positivity'] = df['positivity'].apply(lambda x: round(x,5))
    df = df.sort_values(by=["datetime", "candidate"])
    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--saf',help='service_account_file')
    parser.add_argument('--project_id')
    parser.add_argument('--table')
    args = parser.parse_args()
    query = f"""
        SELECT * FROM {args.table}
    """
    df = load_data(args.saf,args.project_id,query=query)
    df = process_data(df)
    print(df.shape)
    print(df)
