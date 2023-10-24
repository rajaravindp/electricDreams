from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='tslaDB_to_PostGres_v1',
    default_args=default_args,
    description='Push tsla data to postGres',
    schedule_interval= '0 0 * * *',
    start_date=datetime(2023, 10, 21),
) as dag: 

    # Task 1: Create the PostgreSQL table if it doesn't exist
    create_table_task = PostgresOperator(
        task_id="create_pg_table",
        postgres_conn_id="postGres_localHost",
        sql="""
        CREATE TABLE IF NOT EXISTS tsla_listings1 (
            model VARCHAR(255),
            year INT,
            odometer INT,
            price INT,
            DAS VARCHAR(255),
            accident_history TEXT,
            paintJob VARCHAR(255),
            wheels VARCHAR(255),
            emi INT,
            zipCode INT,
            interior VARCHAR(255),
            driveType VARCHAR(255),
            state VARCHAR(255)
        );
        """
    )

    data_file_path = os.path.join(
        os.path.dirname(__file__),
        "tsla_Db_for_mysql_data.csv"
    )
    data = pd.read_csv(data_file_path)

    def insert_data_to_pg():
        # Establish a connection to your PostgreSQL database
        hook = PostgresHook(postgres_conn_id="postGres_localHost")
        conn = hook.get_conn()

        # Create a cursor object using the connection
        cursor = conn.cursor()

        # Define your SQL query
        sql_query = """
        INSERT INTO tsla_listings1 (
            model, year, odometer, price, DAS, accident_history,
            paintJob, wheels, emi, zipCode, interior, driveType, state
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        # Define the parameters for the SQL query
        params = [(row['model'], row['year'], row['odometer'], row['price'],
                row['DAS'], row['accident_history'],
                row['paintJob'], row['wheels'], row['emi'], row['zipCode'],
                row['interior'], row['driveType'], row['state'])
                for index, row in data.iterrows()]

        # Execute the bulk insert operation
        cursor.executemany(sql_query, params)

        # Commit the transaction
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()

    # Task 2: Insert data into the PostgreSQL table using PythonOperator
    insert_data_task = PythonOperator(
        task_id='insert_data_to_pg',
        python_callable=insert_data_to_pg,
        dag=dag,
    )

    # Set task dependencies
    create_table_task >> insert_data_task
