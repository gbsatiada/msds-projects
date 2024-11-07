from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
import pandas as pd
from io import StringIO

@dag(
    dag_id='project_pipeline',
    start_date=datetime(2024, 9, 16),
    schedule='@daily'
)
def etl_pipeline():

    @task
    def extract_postgres_table(table_name, output_path):
        hook = PostgresHook(postgres_conn_id="bgndb")
        sql = f"SELECT * FROM {table_name}"
        df = hook.get_pandas_df(sql)
        
        # Save to S3
        s3_hook = S3Hook(aws_conn_id='data_engineers')
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=output_path,
            bucket_name='de2024-project',
            replace=True
        )
    
    @task
    def extract_dynamodb_table(table_name, output_path):
        dynamodb = DynamoDBHook(aws_conn_id="data_engineers")
        table = dynamodb.get_conn().Table(table_name)
        response = table.scan()
        data = response['Items']
        df = pd.DataFrame(data)
        
        # Save to S3
        s3_hook = S3Hook(aws_conn_id='data_engineers')
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=output_path,
            bucket_name='de2024-project',
            replace=True
        )
    
    @task
    def check_files_exist(file_paths):
        s3_hook = S3Hook(aws_conn_id='data_engineers')
        missing_files = [path for path in file_paths if not s3_hook.check_for_key(key=path, bucket_name='de2024-project')]
        if missing_files:
            raise FileNotFoundError(f"Missing files: {', '.join(missing_files)}")

    @task
    def merge_tables():
        s3_hook = S3Hook(aws_conn_id='data_engineers')
        passenger_trips = pd.read_csv(s3_hook.download_file('landing/passenger_trips.csv', 'de2024-project'))
        jeepney = pd.read_csv(s3_hook.download_file('landing/operation/jeepney.csv', 'de2024-project'))
        driver = pd.read_csv(s3_hook.download_file('landing/drivers/drivers.csv', 'de2024-project'))
        route = pd.read_csv(s3_hook.download_file('landing/operation/route.csv', 'de2024-project'))
        pudo = pd.read_csv(s3_hook.download_file('landing/operation/pickup_dropoff.csv', 'de2024-project'))
        parts = pd.read_csv(s3_hook.download_file('landing/operation/parts.csv', 'de2024-project'))

        merged_df = (passenger_trips.merge(jeepney, on='jeep_id', how='left')
                                   .merge(driver, on='driver_id', how='left')
                                   .merge(route, on='route_id', how='left')
                                   .merge(pudo, on='pudo_id', how='left'))
        
        # Save merged data (with sensitive info)
        csv_buffer = StringIO()
        merged_df.to_csv(csv_buffer, index=False)
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key='sensitive/sensitive_merged_data.csv',
            bucket_name='de2024-project',
            replace=True
        )
        
        sensitive_columns = ['first_name', 'last_name', 'birthday', 'contact_number', 'license_number']
        non_sensitive_df = merged_df.drop(columns=sensitive_columns)
        
        csv_buffer = StringIO()
        non_sensitive_df.to_csv(csv_buffer, index=False)
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key='work/merged_data.csv',
            bucket_name='de2024-project',
            replace=True
        )
        
        jeep_ids = passenger_trips['jeep_id'].unique()
        filtered_parts = parts[parts['jeep_id'].isin(jeep_ids)]
        
        csv_buffer = StringIO()
        filtered_parts.to_csv(csv_buffer, index=False)
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key='work/filtered_parts.csv',
            bucket_name='de2024-project',
            replace=True
        )

    @task
    def extract_and_load_to_redshift():
        s3_hook = S3Hook(aws_conn_id='data_engineers')
        redshift_hook = RedshiftSQLHook(redshift_conn_id='bgnrsdb')

        s3_key_merged = 'work/merged_data.csv'
        s3_key_filtered_parts = 'work/filtered_parts.csv'

        csv_data_merged = s3_hook.read_key(key=s3_key_merged, bucket_name='de2024-project')
        df_merged = pd.read_csv(StringIO(csv_data_merged))
        
        csv_data_filtered_parts = s3_hook.read_key(key=s3_key_filtered_parts, bucket_name='de2024-project')
        df_filtered_parts = pd.read_csv(StringIO(csv_data_filtered_parts))

        df_merged = df_merged.astype({
            'jeep_id': 'Int32',
            'model_year': 'Int32',
            'capacity': 'Int32',
            'route_id_x': 'Int32',
            'pudo_id': 'Int32',
            'driver_id': 'Int32',
            'passenger_travel_duration': 'Int32',
            'is_discount': 'boolean',
            'fare_x': 'float32',
        })
        
        df_filtered_parts = df_filtered_parts.astype({
            'jeep_part_id': 'Int32',
            'part_name': 'str',
            'installation_date': 'str',
            'life_expectancy': 'str',
            'cost': 'float32',
        })

        conn = redshift_hook.get_conn()
        cursor = conn.cursor()

        table_insert_sql = {
            'dim_pickup_dropoff': """
                INSERT INTO dim_pickup_dropoff (
                    pudo_id, route_id, pickup, dropoff, fare
                ) VALUES (%s, %s, %s, %s, %s);
            """,
            'dim_jeepney_part': """
                INSERT INTO dim_jeepney_part (
                    jeepney_part_id, part_name, installation_date, life_expectancy, cost
                ) VALUES (%s, %s, %s, %s, %s);
            """,
            'dim_jeepney': """
                INSERT INTO dim_jeepney (
                    jeepney_id, model_year, capacity
                ) VALUES (%s, %s, %s);
            """,
            'dim_route': """
                INSERT INTO dim_route (
                    route_id, start_location, end_location
                ) VALUES (%s, %s, %s);
            """,
            'fact_trips': """
                INSERT INTO fact_trips (
                    date, jeep_id, driver_id, route_id, pudo_id, card_number,
                    passenger_travel_duration, is_discount, fare
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

        }

        table_columns = {
            'dim_pickup_dropoff': ['pudo_id', 'route_id_y', 'pickup', 'dropoff', 'fare_y'],
            'dim_jeepney_part': ['jeep_part_id', 'part_name', 'installation_date', 'life_expectancy', 'cost'],
            'dim_jeepney': ['jeep_id', 'model_year', 'capacity'],
            'dim_route': ['route_id_x', 'start_location', 'end_location'],
            'fact_trips': ['date', 'jeep_id', 'driver_id', 'route_id_x', 'pudo_id', 'card_id',
                           'passenger_travel_duration', 'is_discount', 'fare_x']
        }
        for table, columns in table_columns.items():
            if table == 'dim_jeepney_part':
                df_table = df_filtered_parts[columns]
            else:
                df_table = df_merged[columns]

            data_tuples = [tuple(row) for row in df_table.to_numpy()]

            cursor.executemany(table_insert_sql[table], data_tuples)
            conn.commit()

        cursor.close()
        conn.close()

    @task
    def drop_duplicates_from_dim_tables():
        redshift_hook = RedshiftSQLHook(redshift_conn_id='bgnrsdb')
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()
    
        dedup_sql = {
            'dim_pickup_dropoff': """
                DELETE FROM dim_pickup_dropoff
                WHERE (pudo_id, route_id, pickup, dropoff, fare) NOT IN (
                    SELECT pudo_id, route_id, pickup, dropoff, fare
                    FROM (
                        SELECT pudo_id, route_id, pickup, dropoff, fare,
                               ROW_NUMBER() OVER (PARTITION BY pudo_id, route_id, pickup, dropoff, fare ORDER BY pudo_id) AS row_num
                        FROM dim_pickup_dropoff
                    ) tmp
                    WHERE row_num = 1
                );
            """,
            'dim_jeepney_part': """
                DELETE FROM dim_jeepney_part
                WHERE (jeepney_part_id, part_name, installation_date, life_expectancy, cost) NOT IN (
                    SELECT jeepney_part_id, part_name, installation_date, life_expectancy, cost
                    FROM (
                        SELECT jeepney_part_id, part_name, installation_date, life_expectancy, cost,
                               ROW_NUMBER() OVER (PARTITION BY jeepney_part_id, part_name, installation_date, life_expectancy, cost ORDER BY jeepney_part_id) AS row_num
                        FROM dim_jeepney_part
                    ) tmp
                    WHERE row_num = 1
                );
            """,
            'dim_jeepney': """
                DELETE FROM dim_jeepney
                WHERE (jeepney_id, model_year, capacity) NOT IN (
                    SELECT jeepney_id, model_year, capacity
                    FROM (
                        SELECT jeepney_id, model_year, capacity,
                               ROW_NUMBER() OVER (PARTITION BY jeepney_id, model_year, capacity ORDER BY jeepney_id) AS row_num
                        FROM dim_jeepney
                    ) tmp
                    WHERE row_num = 1
                );
            """,
            'dim_route': """
                DELETE FROM dim_route
                WHERE (route_id, start_location, end_location) NOT IN (
                    SELECT route_id, start_location, end_location
                    FROM (
                        SELECT route_id, start_location, end_location,
                               ROW_NUMBER() OVER (PARTITION BY route_id, start_location, end_location ORDER BY route_id) AS row_num
                        FROM dim_route
                    ) tmp
                    WHERE row_num = 1
                );
            """
        }
    
        for table, sql in dedup_sql.items():
            try:
                cursor.execute(sql)
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e
    
        cursor.close()
        conn.close()

    @task
    def calculate_and_save_aggregates():
        # Load CSV data from S3
        s3_hook = S3Hook(aws_conn_id='data_engineers')
        merged_df = pd.read_csv(s3_hook.download_file('work/merged_data.csv', 'de2024-project'))

        # Calculate total fare per jeep
        total_fare_per_jeep = merged_df.groupby('jeep_id')['fare_x'].sum().reset_index()
        total_fare_per_jeep.columns = ['jeep_id', 'total_fare']
        csv_buffer = StringIO()
        total_fare_per_jeep.to_csv(csv_buffer, index=False)
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key='gold/total_fare_per_jeep.csv',
            bucket_name='de2024-project',
            replace=True
        )         
        
        # Calculate total no discount fare per jeep
        total_no_discount_per_jeep = merged_df[~merged_df['is_discount']].groupby('jeep_id')['fare_x'].sum().reset_index()
        total_no_discount_per_jeep.columns = ['jeep_id', 'total_no_discount']
        csv_buffer = StringIO()
        total_no_discount_per_jeep.to_csv(csv_buffer, index=False)
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key='gold/total_no_discount_per_jeep.csv',
            bucket_name='de2024-project',
            replace=True
        )
        
        # Calculate average passenger travel duration per route
        avg_passenger_travel_duration_per_route = merged_df.groupby('route_id_x')['passenger_travel_duration'].mean().reset_index()
        avg_passenger_travel_duration_per_route.columns = ['route_id', 'avg_passenger_travel_duration']
        csv_buffer = StringIO()
        avg_passenger_travel_duration_per_route.to_csv(csv_buffer, index=False)
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key='gold/avg_passenger_travel_duration_per_route.csv',
            bucket_name='de2024-project',
            replace=True
        )
        
    # Define task dependencies
    extract_jeepney = extract_postgres_table('jeepney', 'landing/operation/jeepney.csv')
    extract_parts = extract_postgres_table('parts', 'landing/operation/parts.csv')
    extract_route = extract_postgres_table('route', 'landing/operation/route.csv')
    extract_pudo = extract_postgres_table('pickup_dropoff', 'landing/operation/pickup_dropoff.csv')
    extract_drivers = extract_dynamodb_table('drivers', 'landing/drivers/drivers.csv')
    extract_trips = extract_postgres_table('trips', 'landing/operation/trips.csv')

    check_files = check_files_exist([
        'landing/operation/jeepney.csv',
        'landing/operation/parts.csv',
        'landing/operation/route.csv',
        'landing/operation/pickup_dropoff.csv',
        'landing/operation/trips.csv',
        'landing/passenger_trips.csv',
        'landing/drivers/drivers.csv'
    ])

    merge = merge_tables()
    load_to_redshift = extract_and_load_to_redshift()
    drop_duplicates = drop_duplicates_from_dim_tables()
    calc_agg = calculate_and_save_aggregates()


    # Set task dependencies
    [extract_jeepney, extract_parts, extract_route, extract_pudo, extract_drivers, extract_trips] >> check_files

    check_files >> merge >> load_to_redshift >> drop_duplicates >> calc_agg

etl_pipeline()



