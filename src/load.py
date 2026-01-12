import json
import logging
import pandas as pd
from sqlalchemy import create_engine, text, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base


logger = logging.getLogger(__name__)

# --- SQLAlchemy Model Definitions ---
Base = declarative_base()


class Signal(Base):
    __tablename__ = 'signal'
    
    # 'Integer' with 'primary_key=True' automatically becomes SERIAL in Postgres
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)


class Data(Base):
    __tablename__ = 'data'

    # Composite Primary Key
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    signal_id = Column(Integer, ForeignKey('signal.id'), primary_key=True)
    value = Column(Float)


def ensure_database(user, password, host, port, db_name='delfos-target'):
    """
    Ensures that the specified database exists; creates it if missing.
    """
    
    logger.info("Ensuring database exists...")

    # Connect to 'postgres' (maintenance DB) to manage databases
    default_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/postgres"
    default_engine = create_engine(default_url, isolation_level="AUTOCOMMIT")

    with default_engine.connect() as conn:
        # Check if database exists
        result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname='{db_name}'"))
        if not result.fetchone():
            logger.info(f"Database '{db_name}' not found. Creating...")
            conn.execute(text(f'CREATE DATABASE "{db_name}"'))
            logger.info(f"Database '{db_name}' created successfully.")
        else:
            logger.info(f"Database '{db_name}' already exists.")


def ensure_tables(user, password, host, port, db_name='delfos-target'):
    """
    Ensures that the required tables exist in the database and returns the engine.
    """
    # Connect to the target database
    target_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    target_engine = create_engine(target_url)

    # Create tables if missing
    Base.metadata.create_all(target_engine) # skips existing tables automatically
    logger.info("Schema validation complete (tables created if missing).")

    return target_engine
    


def load_data(df, engine):
    """
    Takes a long-format DataFrame (timestamp, name, value),
    upserts new signal names, maps them to IDs, and loads data.
    """

    # Ensure all names are in the 'signal' table
    unique_names = df['name'].unique()
    
    # We use raw SQL for the ON CONFLICT clause (Standard in Postgres)
    logger.debug(f"Upserting {len(unique_names)} unique signal names...")

    with engine.begin() as conn:
        for name in unique_names:
            query = text("""
                INSERT INTO signal (name) VALUES (:name) 
                ON CONFLICT (name) DO NOTHING
            """)
            conn.execute(query, {"name": name})

    logger.debug("Signal names upserted successfully.")

    # Fetch ID Map
    db_signals = pd.read_sql("SELECT name, id FROM signal", engine)
    name_to_id_map = dict(zip(db_signals['name'], db_signals['id']))
    logger.debug(f"Fetched {len(name_to_id_map)} signal IDs from database.")

    # Map IDs in DataFrame
    df['signal_id'] = df['name'].map(name_to_id_map)

    final_df = df[['timestamp', 'signal_id', 'value']]
    logger.debug(f"Prepared final DataFrame with {len(final_df)} rows for loading.")
    logger.debug(f"First rows of final DataFrame:\n{final_df.head()}")

    # Bulk Upload
    # method='multi' allows inserting multiple rows in a single SQL statement (faster)
    final_df.to_sql(
        'data', 
        engine, 
        if_exists='append', 
        index=False, 
        chunksize=5000, 
        method='multi' 
    )
    return final_df


if __name__ == "__main__":
    with open('config.json', 'r') as f:
        DATABASE_CONFIG = json.load(f)

    user = DATABASE_CONFIG['username']
    passwd = DATABASE_CONFIG['password'] 
    host = DATABASE_CONFIG['host']
    port = DATABASE_CONFIG['port']

    ensure_database(user, passwd, host, port)
    db_engine = ensure_tables(user, passwd, host, port)
    
    if db_engine:
        # Simulate Data 
        dates = pd.date_range(start='2023-01-01', periods=5, freq='10min')
        dummy_df = pd.DataFrame({
            'timestamp': dates,
            'name': ['temperature_mean', 'temperature_max', 'humidity_min', 'humidity_std', 'temperature_mean'],
            'value': [22.5, 23.0, 45.1, 1.2, 22.8]
        })
        
        load_data(dummy_df, db_engine)