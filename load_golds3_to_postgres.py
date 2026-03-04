import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from urllib.parse import quote_plus   # for URL-encoding the password

# -----------------------------------------------------------------------------
# 1. CONFIGURE YOUR S3 GOLD PATHS
# -----------------------------------------------------------------------------
BUCKET = "world-bank-api"
BASE_PREFIX = "worldbank/gold"

PATH_DIM_COUNTRY             = f"s3://{BUCKET}/{BASE_PREFIX}/dim_country"
PATH_DIM_REGION              = f"s3://{BUCKET}/{BASE_PREFIX}/dim_region"
PATH_DIM_ADMIN_REGION        = f"s3://{BUCKET}/{BASE_PREFIX}/dim_admin_region"
PATH_DIM_INCOME_LEVEL        = f"s3://{BUCKET}/{BASE_PREFIX}/dim_income_level"
PATH_DIM_LENDING_TYPE        = f"s3://{BUCKET}/{BASE_PREFIX}/dim_lending_type"
PATH_DIM_AGGREGATE_ENTITY    = f"s3://{BUCKET}/{BASE_PREFIX}/dim_aggregate_entity"
PATH_FACT_COUNTRY_MEMBERSHIP = f"s3://{BUCKET}/{BASE_PREFIX}/fact_country_membership"

# -----------------------------------------------------------------------------
# 2. POSTGRES CONFIG (LOCALHOST)
# -----------------------------------------------------------------------------
PG_HOST = "localhost"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASSWORD = "XXXXXXX"
PG_DB_NAME = "worldbank"

# Connection strings
# 🔹 psycopg2 DSN uses plain password (NO URL encoding needed here)
PG_ADMIN_CONN_STR = (
    f"dbname=postgres "
    f"user={PG_USER} "
    f"password={PG_PASSWORD} "
    f"host={PG_HOST} "
    f"port={PG_PORT}"
)

# 🔹 SQLAlchemy URL needs URL-encoded password because of '@'
encoded_password = quote_plus(PG_PASSWORD)   # this turns '@' into '%40'
PG_DB_URL = (
    f"postgresql+psycopg2://{PG_USER}:{encoded_password}@{PG_HOST}:{PG_PORT}/{PG_DB_NAME}"
)

print("PG_DB_URL:", PG_DB_URL)  # just to verify once


def ensure_database_exists():
    """Create the 'worldbank' database if it doesn't exist."""
    conn = psycopg2.connect(PG_ADMIN_CONN_STR)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (PG_DB_NAME,))
    exists = cur.fetchone() is not None

    if not exists:
        print(f"Creating database '{PG_DB_NAME}'...")
        cur.execute(f"CREATE DATABASE {PG_DB_NAME}")
    else:
        print(f"Database '{PG_DB_NAME}' already exists.")

    cur.close()
    conn.close()


def load_parquet_from_s3(path: str) -> pd.DataFrame:
    """Load a Parquet folder from S3 into a pandas DataFrame."""
    print(f"Reading Parquet from: {path}")
    df = pd.read_parquet(path)
    print(f"  -> Loaded {len(df)} rows")
    return df


def write_df_to_postgres(df: pd.DataFrame, table_name: str, engine):
    """Write a pandas DataFrame to Postgres using to_sql."""
    print(f"Writing {len(df)} rows to table '{table_name}'...")
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"✅ Wrote table: {table_name}")


def main():
    # Step 1: Ensure DB exists
    ensure_database_exists()

    # Step 2: Create SQLAlchemy engine to 'worldbank' DB
    engine = create_engine(PG_DB_URL)

    # Step 3: Read Gold Parquet dims & fact from S3
    dim_country_df = load_parquet_from_s3(PATH_DIM_COUNTRY)
    dim_region_df = load_parquet_from_s3(PATH_DIM_REGION)
    dim_admin_region_df = load_parquet_from_s3(PATH_DIM_ADMIN_REGION)
    dim_income_level_df = load_parquet_from_s3(PATH_DIM_INCOME_LEVEL)
    dim_lending_type_df = load_parquet_from_s3(PATH_DIM_LENDING_TYPE)
    dim_aggregate_entity_df = load_parquet_from_s3(PATH_DIM_AGGREGATE_ENTITY)
    fact_country_membership_df = load_parquet_from_s3(PATH_FACT_COUNTRY_MEMBERSHIP)

    # Step 4: Write them into Postgres (Gold warehouse)
    write_df_to_postgres(dim_country_df,             "dim_country", engine)
    write_df_to_postgres(dim_region_df,              "dim_region", engine)
    write_df_to_postgres(dim_admin_region_df,        "dim_admin_region", engine)
    write_df_to_postgres(dim_income_level_df,        "dim_income_level", engine)
    write_df_to_postgres(dim_lending_type_df,        "dim_lending_type", engine)
    write_df_to_postgres(dim_aggregate_entity_df,    "dim_aggregate_entity", engine)
    write_df_to_postgres(fact_country_membership_df, "fact_country_membership", engine)

    print("🎉 All Gold tables loaded into Postgres 'worldbank'.")


if __name__ == "__main__":
    main()
