"""extract data from external source and record to OLTP"""
import json
import ssl
import pandas as pd
from sqlalchemy import create_engine

ssl._create_default_https_context = ssl._create_unverified_context

branches_map = {
    'A': 1,
    'B': 2,
    'C': 3
}

map_customer_type = {
    'Member': 1,
    'Normal': 2
}

map_payment = {
    'Ewallet': 1,
    'Cash': 2,
    'Credit card': 3
}

map_product_line = {
    'Health and beauty': 1,
    'Electronic accessories': 2,
    'Home and lifestyle': 3,
    'Sports and travel': 4,
    'Food and beverages': 5,
    'Fashion accessories': 6
}

cols_to_rename = {
    'Invoice ID': 'invoice_id',
    'Gender': 'gender',
    'Unit price': 'unit_price',
    'Quantity': 'quantity',
    'Rating': 'rating',
}

cols_to_record = ['invoice_id', 'branch_id', 'customer_type_id', 'payment_type_id',
                  'product_line_id', 'unit_price', 'quantity',
                  'gender', 'invoice_date', 'rating']


def get_secrets(secret_file="/opt/airflow/dags/etl_scripts/secrets.json"):
    """this function retieves secrets"""
    with open(secret_file, "r") as secrets:
        secret_data = json.load(secrets)

    return secret_data["url"], secret_data["nds_conn_string"]


def get_data(url):
    """this function retieves data from external source"""
    return pd.read_csv(url)


def preprocess_data(df):
    """this function performs data transformations"""

    df['branch_id'] = df['Branch'].map(branches_map)
    df['customer_type_id'] = df['Customer type'].map(map_customer_type)
    df['payment_type_id'] = df['Payment'].map(map_payment)
    df['product_line_id'] = df['Product line'].map(map_product_line)
    df['invoice_date'] = pd.to_datetime(
        df['Date'] + ' ' + df['Time'], format='%m/%d/%Y %H:%M')
    df.rename(cols_to_rename, axis=1, inplace=True)
    return df[cols_to_record]


def record_to_nds(df, con_str):
    """this function records data to OLTP"""

    engine = create_engine(con_str)
    df[cols_to_record].to_sql('invoices',
                              engine, schema='supermarket_project',
                              if_exists='append', index=False)


def main():
    """the main function"""
    url, con_str = get_secrets()
    df = get_data(url)
    df = preprocess_data(df)
    record_to_nds(df, con_str)


if __name__ == '__main__':
    main()
