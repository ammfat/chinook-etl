# Chinook ETL

## Project Description

This is an ETL (Extract, Transform, Load) project that uses the Chinook database as the data source.

The Chinook database is a sample database that represents a digital media store. The database contains information about the artists, songs, and albums from the music store, as well as information on the store's employees, customers, and the customers purchases.


## Project Goal

The goal of this project is to create an ETL pipeline that will extract data from the Chinook database, transform the data, and load the data into a Google Cloud Platform (GCP) BigQuery data warehouse. The data warehouse will be used to analyze the data and answer business questions.


## Data Source Setup

1. Go to `data/` directory.

1. Login to your local PostgreSQL.

1. Dump the Chinook SQL schema by running:
    
    ```
    \i chinook_pg.sql
    ```

1. To check if the database is created, run the following command:
    
    ```
    SELECT tablename FROM pg_catalog.pg_tables 
    WHERE schemaname != 'pg_catalog' 
    AND schemaname != 'information_schema';
    ```

    Ensure there are 11 tables listed.

## GCP Setup

Coming Soon...


## Airflow Installation Setup

1. Set up virtual environment and install dependencies.
    
    ```
    python3 -m venv venv
    source venv/bin/activate
    ```

1. Setup environment variables.

    ```
    echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
    ```

1. Add Airflow's internal Postgres and GCP resources to the `.env`. Follow `.env.example` for reference.

1. Run docker container for Airflow.

    ```
    docker-compose up --build -d
    ```

1. Go to Airflow Web UI at http://localhost:8080/ to ensure the container is running.


## Airflow Connections

To connect Airflow with external system, we need to setup connections on Airflow.

1. On http://localhost:8080, go to **Admin > Connections**.  Add Google Cloud connection type.

    - **Conn Id**: `google_cloud_conn_id`
    - **Conn Type**: `Google Cloud Platform`
    - **Project Id**: `<your-project-id>`
    - **Keyfile Path**: `/creds/<your-sa-keyfile>.json`
    - **Scopes**: `https://www.googleapis.com/auth/cloud-platform`

1. Add another connection for the local PostgreSQL.

    - **Conn Id**: `postgres_conn_id`
    - **Conn Type**: `Postgres`
    - **Host**: `<your-local-ip>` (e.g.: 192.x.x.x)
    - **Schema**: `<your-local-postgres-schema>`
    - **Login**: `<your-local-postgres-username>`
    - **Password**: `<your-local-postgres-password>`
    - **Port**: `5432`


## Run Airflow DAG

Go to http://localhost:8080 to turn on the DAG.

The DAG scheduled to run `@monthly` from inclusive range of `2009-01-01T05:00` to `2013-12-01T05:00`.


## Acknowledgements

- **ETL Flatfile Airflow** by [@okzapradhana]((https://github.com/okzapradhana/etl-flatfile-airflow))

- **Chinook Database** by [@lerocha](https://github.com/lerocha/chinook-database)
