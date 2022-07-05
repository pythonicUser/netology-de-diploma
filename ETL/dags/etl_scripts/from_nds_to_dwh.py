"""extract data from OLTP and record to OLAP"""
from datetime import timedelta
import json
import pandas as pd
from sqlalchemy import create_engine
from clickhouse_driver import Client


def get_secrets(secret_file="/opt/airflow/dags/etl_scripts/secrets.json"):
    """this function retieves secrets source"""

    with open(secret_file, "r") as secrets:
        secret_data = json.load(secrets)
        nds_con = secret_data["nds_conn_string"]
        dwh_con = secret_data["dwh_conn_string"]
        fields = secret_data["attrs_required"]
    return nds_con, dwh_con, fields


def get_last_date(client):
    """this function obtains last date of a record in DWH"""

    last_date = client.execute(
        "SELECT MAX(created_at_nds) as last_date FROM invoices_fct")
    modified_date = list(last_date[0])[0] + timedelta(seconds=1)
    return modified_date.strftime("%Y-%m-%d %H:%M:%S")


def get_new_data_from_nds(last_date, conn_str, attrs=None):
    """this function retrieves data from OLTP storage"""

    engine = create_engine(conn_str)
    attrs = ','.join(attrs)
    sql = f"select {attrs} from supermarket_project.nds_mart where created_at_nds > '{last_date}'"
    data = pd.read_sql(sql=sql, con=engine)
    data['invoice_id'] = 'dwh-' + data['invoice_id']
    return data


def insert_to_ch(df, client):
    """this function records data into DWH"""

    client.execute("INSERT INTO invoices_fct VALUES", df.to_dict('records'))


def main():
    """the main function"""
    nds_conn_string, dwh_conn_string, attrs = get_secrets()
    client = Client(dwh_conn_string)
    last_date = get_last_date(client)
    print(last_date)
    df = get_new_data_from_nds(last_date, nds_conn_string, attrs)
    print(df.shape)
    insert_to_ch(df, client)


if __name__ == '__main__':
    main()
