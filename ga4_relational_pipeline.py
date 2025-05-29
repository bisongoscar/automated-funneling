import os
import sqlite3
import logging
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from tenacity import retry, stop_after_attempt, wait_exponential

# Load environment variables
load_dotenv('.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ga4_data_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'ga4': {
        'property_id': os.getenv('GA4_PROPERTY_ID'),
        'credentials': os.getenv('GA4_CREDENTIALS_PATH')
    },
    'database': {
        'path': 'ga4_data.db',
        'tables': {
            'dates': 'dates',
            'users': 'user_interaction',
            'content': 'content_metrics',
            'site': 'site_data'
        }
    }
}


def init_db():
    """Initialize SQLite database with relational schema"""
    conn = sqlite3.connect(CONFIG['database']['path'])
    cursor = conn.cursor()
    
    # Create tables with relationships
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dates (
            date_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE NOT NULL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_interaction (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_id INTEGER NOT NULL,
            users INTEGER,
            sessions INTEGER,
            engagement_rate REAL,
            conversions INTEGER,
            average_session_duration REAL,
            FOREIGN KEY (date_id) REFERENCES dates (date_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS content_metrics (
            content_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_id INTEGER NOT NULL,
            page_title TEXT,
            page_views INTEGER,
            content_sessions INTEGER,
            engagement_rate REAL,
            session_duration REAL,
            FOREIGN KEY (date_id) REFERENCES dates (date_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS site_data (
            site_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_id INTEGER NOT NULL,
            search_Term TEXT,
            clicks INTEGER,
            impressions INTEGER,
            FOREIGN KEY (date_id) REFERENCES dates (date_id)
        )
    ''')
    
    conn.commit()
    conn.close()
    logger.info("Database initialized with relational schema")

def get_or_create_date_id(date_str):
    """Get or create date entry and return date_id"""
    conn = sqlite3.connect(CONFIG['database']['path'])
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT date_id FROM dates WHERE date = ?
        ''', (date_str,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            cursor.execute('''
                INSERT INTO dates (date) VALUES (?)
            ''', (date_str,))
            conn.commit()
            return cursor.lastrowid
    except Exception as e:
        logger.error(f"Date ID lookup failed: {str(e)}")
        raise
    finally:
        conn.close()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_ga4_data(start_date: str, end_date: str):
    """Fetch GA4 data for all metric categories"""
    client = BetaAnalyticsDataClient.from_service_account_json(CONFIG['ga4']['credentials'])
    
    try:
        # User Interaction Data
        user_request = RunReportRequest(
            property=f"properties/{CONFIG['ga4']['property_id']}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="date")],
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="sessions"),
                Metric(name="engagementRate"),
                Metric(name="conversions"),
                Metric(name="averageSessionDuration")
            ]
        )
        
        # Content Data
        content_request = RunReportRequest(
            property=f"properties/{CONFIG['ga4']['property_id']}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="date"), Dimension(name="pageTitle")],
            metrics=[
                Metric(name="screenPageViews"),
                Metric(name="sessions"),
                Metric(name="engagementRate"),
                Metric(name="userEngagementDuration")
            ]
        )
        
        # Site Data
        site_request = RunReportRequest(
            property=f"properties/{CONFIG['ga4']['property_id']}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="date"), Dimension(name="searchTerm")],
            metrics=[
                Metric(name="eventCount"),
                Metric(name="screenPageViews")
            ]
        )
        
        # Process responses
        user_response = client.run_report(user_request)
        content_response = client.run_report(content_request)
        site_response = client.run_report(site_request)
        
        def process_response(response, type):
            data = []
            for row in response.rows:
                ga4_date = row.dimension_values[0].value
                formatted_date = f"{ga4_date[:4]}-{ga4_date[4:6]}-{ga4_date[6:8]}"
                entry = {'date': formatted_date}
                
                if type == 'user':
                    entry.update({
                        'users': int(row.metric_values[0].value),
                        'sessions': int(row.metric_values[1].value),
                        'engagement_rate': float(row.metric_values[2].value),
                        'conversions': int(row.metric_values[3].value),
                        'average_session_duration': float(row.metric_values[4].value)
                    })
                elif type == 'content':
                    entry.update({
                        'page_title': row.dimension_values[1].value,
                        'page_views': int(row.metric_values[0].value),
                        'sessions': int(row.metric_values[1].value),
                        'engagement_rate': float(row.metric_values[2].value),
                        'session_duration': float(row.metric_values[3].value)
                    })
                elif type == 'site':
                    entry.update({
                        'search_Term': row.dimension_values[1].value,
                        'clicks': int(row.metric_values[0].value),
                        'impressions': int(row.metric_values[1].value)
                    })
                
                data.append(entry)
            return pd.DataFrame(data)
        
        return {
            'users': process_response(user_response, 'user'),
            'content': process_response(content_response, 'content'),
            'site': process_response(site_response, 'site')
        }
        
    except Exception as e:
        logger.error(f"GA4 data fetch failed: {str(e)}")
        raise

def save_data(data):
    """Save data to appropriate tables with relational integrity"""
    conn = sqlite3.connect(CONFIG['database']['path'])
    cursor = conn.cursor()
    
    try:
        # Get unique dates from all datasets
        all_dates = set()
        for df in data.values():
            all_dates.update(df['date'].unique())
        
        # Create date entries first
        date_map = {}
        for date_str in all_dates:
            date_id = get_or_create_date_id(date_str)
            date_map[date_str] = date_id
        
        # Insert user data
        for _, row in data['users'].iterrows():
            cursor.execute('''
                INSERT INTO user_interaction (
                    date_id, users, sessions, engagement_rate, conversions, average_session_duration
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                date_map[row['date']],
                row['users'],
                row['sessions'],
                row['engagement_rate'],
                row['conversions'],
                row['average_session_duration']
            ))
        
        # Insert content data
        for _, row in data['content'].iterrows():
            cursor.execute('''
                INSERT INTO content_metrics (
                    date_id, page_title, page_views, content_sessions, engagement_rate, session_duration
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                date_map[row['date']],
                row['page_title'],
                row['page_views'],
                row['sessions'],
                row['engagement_rate'],
                row['session_duration']
            ))
        
        # Insert site data
        for _, row in data['site'].iterrows():
            cursor.execute('''
                INSERT INTO site_data (
                    date_id, search_Term, clicks, impressions
                ) VALUES (?, ?, ?, ?)
            ''', (
                date_map[row['date']],
                row['search_Term'],
                row['clicks'],
                row['impressions']
            ))
        
        conn.commit()
        logger.info(f"Successfully saved data for {len(all_dates)} dates")
        
        # Export to CSV
        for table in ['users', 'content', 'site']:
            data[table].to_csv(f"{table}_metrics.csv", index=False)
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Data save failed: {str(e)}")
        raise
    finally:
        conn.close()

def get_last_date():
    """Get the last available date from database"""
    conn = sqlite3.connect(CONFIG['database']['path'])
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT MAX(date) FROM dates
        ''')
        last_date = cursor.fetchone()[0]
        
        if last_date:
            # Handle both formats during transition
            try:
                return datetime.strptime(last_date, '%Y-%m-%d').date()
            except ValueError:
                # Fallback to old format if needed
                return datetime.strptime(last_date, '%Y%m%d').date()
        return None
    except Exception as e:
        logger.error(f"Date retrieval failed: {str(e)}")
        return None
    finally:
        conn.close()



def main():
    """Main function to run the pipeline"""
    logger.info("Starting GA4 relational data pipeline")
    init_db()
    
    try:
        # Determine date range
        last_date = get_last_date()
        end_date = datetime.now().date()
        
        if last_date:
            start_date = last_date + timedelta(days=1)
        else:
            start_date = end_date - timedelta(days=30)
        
        if start_date > end_date:
            logger.info("No new data to fetch")
            return
            
        logger.info(f"Fetching data from {start_date} to {end_date}")
        data = fetch_ga4_data(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        if not any(not df.empty for df in data.values()):
            logger.info("No data returned from GA4 API")
            return
            
        save_data(data)
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()