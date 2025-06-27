import os
import pymssql
import logging
from dotenv import load_dotenv
from utils import log_event
from pattern_checking import process_row

# Load environment variables
load_dotenv()

# Database Configuration
DB_CONFIG = {
    "server": os.getenv("DB_SERVER"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}


def create_table():
    """Ensure the Products table exists and contains all necessary columns."""
    try:
        with pymssql.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                # Step 1: Create the table if it doesn't exist
                create_table_query = """
                IF NOT EXISTS (
                    SELECT * FROM Webstudy.INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = 'IBM_Algo_Webstudy_Products' 
                    AND TABLE_SCHEMA = 'dbo'
                )
                BEGIN
                    CREATE TABLE dbo.IBM_Algo_Webstudy_Products (
                        unique_id NVARCHAR(255) PRIMARY KEY,
                        CurrentDate DATETIME,
                        Header NVARCHAR(255),
                        ProductName NVARCHAR(255),
                        ImagePath NVARCHAR(MAX),
                        Kt NVARCHAR(255),  
                        Price NVARCHAR(255),
                        TotalDiaWt NVARCHAR(255),
                        Time DATETIME DEFAULT GETDATE(),
                        AdditionalInfo NVARCHAR(MAX) NULL
                    )
                END
                """
                cursor.execute(create_table_query)

                # Step 2: Add 'AdditionalInfo' column if not exists
                add_column_query = """
                IF NOT EXISTS (
                    SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'IBM_Algo_Webstudy_Products' 
                    AND COLUMN_NAME = 'AdditionalInfo'
                )
                BEGIN
                    ALTER TABLE dbo.IBM_Algo_Webstudy_Products 
                    ADD AdditionalInfo NVARCHAR(MAX) NULL
                END
                """
                cursor.execute(add_column_query)

                conn.commit()
                logging.info("Table and column 'AdditionalInfo' checked/created successfully.")
    except pymssql.DatabaseError as e:
        logging.error(f"Database error: {e}")



def insert_into_db(data):
    """Insert scraped data into the MSSQL database with enhanced Kt and TotalDiaWt validation."""
    if not data:
        log_event("No data to insert into the database.")
        return
    
    try:
        with pymssql.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                query = """
                    INSERT INTO dbo.IBM_Algo_Webstudy_Products 
                    (unique_id, CurrentDate, Header, ProductName, ImagePath, Kt, Price, TotalDiaWt, AdditionalInfo)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                processed_data = [process_row(row) for row in data]
                
                cursor.executemany(query, processed_data)
                conn.commit()
                logging.info(f"Inserted {len(processed_data)} records successfully.")
                
    except pymssql.DatabaseError as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

# Function to fetch scraping settings
def get_scraping_settings():
    """Fetches current scraping settings from the database."""
    try:
        with pymssql.connect(**DB_CONFIG) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute("""
                    SELECT monthly_product_limit,products_fetched_month, last_reset
                    FROM dbo.IBM_Algo_Webstudy_scraping_settings
                """)
                data = cursor.fetchone()
                if not data:
                    return {"success": False, "message": "No data found."}
                return {"success": True, "data": data}
    except pymssql.Error as e:
        return {"success": False, "error": f"Database error: {str(e)}"}


create_table()

def reset_scraping_limit():
    """Resets `products_fetched_today` to 0 and `is_disabled` to 0 using pymssql."""
    try:
        with pymssql.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                update_query = """
                    UPDATE dbo.IBM_Algo_Webstudy_scraping_settings
                    SET products_fetched_month = 0, is_disabled = 0
                """
                cursor.execute(update_query)
                conn.commit()

        return {"success": True, "message": "Limits have been reset successfully."}

    except pymssql.Error as e:
        return {"success": False, "error": f"Database error: {str(e)}"}
    except Exception as e:
        return {"success": False, "error": f"Unexpected error: {str(e)}"}
    
    
# scaping all data call
def get_all_scraped_products():
    """Fetches all product data from the database."""
    try:
        with pymssql.connect(**DB_CONFIG) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute("""
                    SELECT [unique_id], [CurrentDate], [Header], [ProductName],
                        [ImagePath], [Kt], [Price], [TotalDiaWt], [Time]
                    FROM dbo.IBM_Algo_Webstudy_Products
                    ORDER BY CurrentDate DESC
                """)
                products = cursor.fetchall()
                if not products:
                    return {"success": False, "message": "No products found."}
                return {"success": True, "data": products}
    except pymssql.Error as e:
        return {"success": False, "error": f"Database error: {str(e)}"}  


#######################################################################################
#===================================================================================================

def create_table_if_not_exists():
    try:
        with pymssql.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                create_table_sql = """
                IF NOT EXISTS (
                    SELECT * FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'IBM_Algo_Webstudy_Products_details'
                )
                BEGIN
                    CREATE TABLE dbo.IBM_Algo_Webstudy_Products_details (
                        unique_id NVARCHAR(100),
                        CurrentDate DATETIME,
                        Header NVARCHAR(255),
                        ProductName NVARCHAR(MAX),
                        SKU NVARCHAR(100),
                        RingSizes NVARCHAR(255),
                        Price NVARCHAR(100),
                        ProtectionPlan NVARCHAR(MAX),
                        MonthlyPayment NVARCHAR(100),
                        AdditionalInfo NVARCHAR(MAX),
                        ReviewSummary NVARCHAR(MAX),
                        ImageUrls NVARCHAR(MAX),
                        ProductURL NVARCHAR(MAX),
                        DateOfScrape DATETIME
                    )
                END
                """
                cursor.execute(create_table_sql)
                conn.commit()
                logging.info("Table 'IBM_Algo_Webstudy_Products_details' checked/created successfully.")
    except pymssql.DatabaseError as e:
        logging.error(f"Database error during table creation: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during table creation: {e}")


def insert_into_db_details(data):
    if not data:
        logging.warning("No data to insert into the database.")
        return

    try:
        with pymssql.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                query = """
                    INSERT INTO dbo.IBM_Algo_Webstudy_Products_details 
                    (
                        unique_id, CurrentDate, Header, ProductName, SKU,
                        RingSizes, Price, ProtectionPlan, MonthlyPayment,
                        AdditionalInfo, ReviewSummary, ImageUrls, ProductURL, DateOfScrape
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(query, data)
                conn.commit()
                logging.info(f"Inserted {len(data)} records successfully.")
    except pymssql.DatabaseError as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
  