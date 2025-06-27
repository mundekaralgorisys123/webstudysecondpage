import os
import pymssql
import logging
from dotenv import load_dotenv
from datetime import date, datetime
 # Missing import
from utils import log_event  # Assuming this is your custom logging function

# Load environment variables
load_dotenv()

# Database Configuration
DB_CONFIG = {
    "server": os.getenv("DB_SERVER"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}
def get_db_connection():
    try:
        conn = pymssql.connect(
            server=DB_CONFIG['server'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database']
        )
        return conn
    except Exception as e:
        log_event(f"Database connection failed: {e}")
        raise

def check_monthly_limit():
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            # Check monthly limit settings
            cursor.execute("""
                SELECT monthly_product_limit, products_fetched_month, 
                       last_reset, is_disabled 
                FROM IBM_Algo_Webstudy_scraping_settings 
                WHERE setting_name = 'monthly_product_limit'
            """)
            result = cursor.fetchone()
            
            if not result:
                log_event("No monthly limit setting found")
                return False
            
            # Convert result to dictionary
            columns = [column[0] for column in cursor.description]
            monthly_data = dict(zip(columns, result))
            
            # Get current date
            current_date = datetime.now().date()
            
            # Extract month/year from last reset
            last_reset_date = monthly_data['last_reset']
            if isinstance(last_reset_date, datetime):
                last_reset_date = last_reset_date.date()
            
            # Check if month has changed
            if last_reset_date.month != current_date.month or last_reset_date.year != current_date.year:
                cursor.execute("""
                    UPDATE IBM_Algo_Webstudy_scraping_settings 
                    SET products_fetched_month = 0, 
                        last_reset = %s,
                        is_disabled = 0
                    WHERE setting_name = 'monthly_product_limit'
                """, (current_date,))
                connection.commit()
                monthly_data['products_fetched_month'] = 0
                monthly_data['is_disabled'] = False
                log_event("Monthly counter reset for new month")
            
            # Check limit
            if monthly_data['products_fetched_month'] >= monthly_data['monthly_product_limit']:
                cursor.execute("""
                    UPDATE IBM_Algo_Webstudy_scraping_settings 
                    SET is_disabled = 1
                    WHERE setting_name = 'monthly_product_limit'
                """)
                connection.commit()
                log_event("Monthly limit reached - scraping disabled")
                return False
            
            return True
            
    except Exception as e:
        log_event(f"Monthly limit check error: {str(e)}")
        return False
    finally:
        connection.close()

def update_product_count(count):
    """Update monthly product count in the database"""
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE IBM_Algo_Webstudy_scraping_settings 
                SET products_fetched_month = products_fetched_month + %s
                WHERE setting_name = 'monthly_product_limit'
            """, (count,))
            connection.commit()
            log_event(f"Updated monthly product count by +{count}")
    except Exception as e:
        connection.rollback()
        log_event(f"Error updating monthly count: {e}")
        raise
    finally:
        connection.close()
        
# if __name__ == "__main__":
#     if check_daily_limit():
#         # Example: If scraping is allowed, increment the counter by the number of products fetched
#         products_fetched = 100  # Replace with actual fetched count
#         update_product_count(products_fetched)



# CREATE TABLE IBM_Algo_Webstudy_scraping_settings (
#     id INT IDENTITY(1,1) PRIMARY KEY,
#     setting_name VARCHAR(100) NOT NULL UNIQUE,
#     daily_limit INT NOT NULL,
#     products_fetched_today INT DEFAULT 0,
#     last_reset DATE NOT NULL,
#     is_disabled BIT DEFAULT 0  -- Use 0 for FALSE
# );


# INSERT INTO IBM_Algo_Webstudy_scraping_settings (
#     setting_name,
#     daily_limit,
#     products_fetched_today,
#     last_reset,
#     is_disabled
# ) VALUES (
#     'daily_product_limit',  -- Setting name for combined limit
#     2000,                   -- Total daily limit of 4000 products
#     0,                      -- Start with 0 products fetched today
#     GETDATE(),              -- Set today's date as last reset
#     0                       -- Not disabled by default
# );



# UPDATE [Webstudy].[dbo].[scraping_settings] 
# SET [daily_limit] = 300
# WHERE [setting_name] = 'daily_product_limit';



# def create_table_and_insert_record():
#     try:
#         with pymssql.connect(**DB_CONFIG) as conn:
#             with conn.cursor() as cursor:
#                 # Create table if not exists
#                 create_table_query = """
#                 IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='IBM_Algo_Webstudy_scraping_settings' AND xtype='U')
#                 CREATE TABLE IBM_Algo_Webstudy_scraping_settings (
#                     id INT IDENTITY(1,1) PRIMARY KEY,
#                     setting_name VARCHAR(100) NOT NULL UNIQUE,
#                     daily_limit INT NOT NULL,
#                     products_fetched_today INT DEFAULT 0,
#                     last_reset DATE NOT NULL,
#                     is_disabled BIT DEFAULT 0  -- Use 0 for FALSE
#                 );
#                 """
#                 cursor.execute(create_table_query)
                
#                 # Insert or update record
#                 insert_query = """
#                 MERGE INTO IBM_Algo_Webstudy_scraping_settings AS target
#                 USING (SELECT 'daily_product_limit' AS setting_name, 2000 AS daily_limit, 0 AS products_fetched_today, CAST(GETDATE() AS DATE) AS last_reset, 0 AS is_disabled) AS source
#                 ON target.setting_name = source.setting_name
#                 WHEN MATCHED THEN
#                     UPDATE SET last_reset = source.last_reset
#                 WHEN NOT MATCHED THEN
#                     INSERT (setting_name, daily_limit, products_fetched_today, last_reset, is_disabled)
#                     VALUES (source.setting_name, source.daily_limit, source.products_fetched_today, source.last_reset, source.is_disabled);
#                 """
#                 cursor.execute(insert_query)
                
#                 # Commit changes
#                 conn.commit()
#                 print("Table created and record inserted successfully.")
#     except Exception as e:
#         print(f"Error: {e}")
#     finally:
#         conn.close()
        
        
# # Call the function
# create_table_and_insert_record()        


# CREATE TABLE scraping_logs (
#     id VARCHAR(100) PRIMARY KEY,         -- Unique hash ID from URL + timestamp
#     url TEXT,                             -- The target URL being scraped
#     ip_address VARCHAR(45),              -- IP address of the client (IPv4 or IPv6)
#     request_time DATETIME,               -- Timestamp of when the scrape was initiated
#     status VARCHAR(20)                   -- Status of the scrape: 'active', 'inactive', or 'error'
# );
