import logging
from typing import Dict, Optional
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from airflow.utils.db import provide_session
from sqlalchemy import text

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='data_collection.log'
)
logger = logging.getLogger(__name__)

class DataCollector:
    def __init__(self, config):
        self.config = config
        # Initialize Google Analytics API client
        self.analytics = self.initialize_google_analytics()
        
    def initialize_google_analytics(self) -> Optional[requests.Session]:
        """
        Initializes a session for Google Analytics API.
        Returns: Session object or None if initialization fails.
        """
        try:
            session = requests.Session()
            # Assume GA_API_KEY is set in environment variables
            session.headers.update({'Authorization': f'Bearer {self.config["GA_API_KEY"]}'})
            return session
        except Exception as e:
            logger.error(f"Failed to initialize Google Analytics client: {e}")
            return None
    
    def collect_click_data(self) -> pd.DataFrame:
        """
        Collects click data from Google Analytics.
        Returns: DataFrame with click data or empty DataFrame if failed.
        """
        try:
            response = requests.get(
                url=self.config['GA_ENDPOINT'],
                params={
                    'start_date': self.config['START_DATE'],
                    'end_date': self.config['END_DATE']
                }
            )
            if not response.ok:
                logger.error(f"Failed to collect click data: {response.status_code}")
                return pd.DataFrame()
            
            data = response.json()
            df = pd.DataFrame(data['items'])
            return df
        except Exception as e:
            logger.error(f"Error collecting click data: {e}")
            return pd.DataFrame()

class DataLoader:
    def __init__(self, config):
        self.config = config
        # Initialize BigQuery client
        self.bigquery_client = self.initialize_bigquery()
    
    def initialize_bigquery(self) -> Optional[bigquery.Client]:
        """
        Initializes a BigQuery client using service account credentials.
        Returns: BigQuery Client or None if initialization fails.
        """
        try:
            credentials = service_account.Credentials.from_service_account_info(
                self.config['GCP_CREDENTIALS']
            )
            return bigquery.Client(credentials=credentials, project=self.config['GCP_PROJECT'])
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            return None
    
    def load_data(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        Loads DataFrame into BigQuery.
        Args:
            df: DataFrame containing data to be loaded
            table_name: Name of the table in BigQuery
        Returns: True if successful, False otherwise.
        """
        try:
            if not self.bigquery_client:
                return False
                
            # Prepare schema for BigQuery
            schema = [
                bigquery.SchemaField("timestamp", "STRING"),
                bigquery.SchemaField("user_id", "STRING"),
                bigquery.SchemaField("affiliate_id", "STRING"),
                bigquery.SchemaField("click_source", "STRING")
            ]
            
            table_ref = self.bigquery_client.dataset(self.config['DATASET']).table(table_name)
            job_config = bigquery.job.JobConfig(
                writeDisposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=schema
            )
            
            # Convert DataFrame to JSON for BigQuery import
            json_data = df.to_json(orient='records')
            self.bigquery_client.insert_rows_json(table_ref, json_data, job_config=job_config)
            return True
        except Exception as e:
            logger.error(f"Error loading data into BigQuery: {e}")
            return False

def etl_pipeline(config):
    """
    ETL pipeline for collecting and loading click data.
    Args:
        config: Dictionary containing configuration parameters
    Returns: None
    """
    try:
        collector = DataCollector(config)
        if not collector.analytics:
            logger.error("Google Analytics client failed to initialize")
            return
            
        click_data = collector.collect_click_data()
        if click_data.empty:
            logger.info("No click data collected")
            return
            
        loader = DataLoader(config)
        if not loader.bigquery_client:
            logger.error("BigQuery client failed to initialize")
            return
            
        success = loader.load_data(click_data, "clicks_table")
        if success:
            logger.info(f"Successfully loaded {len(click_data)} rows into BigQuery")
        else:
            logger.error("Failed to load data into BigQuery")
            
    except Exception as e:
        logger.error(f"Error in ETL pipeline: {e}")