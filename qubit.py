import pyodbc  # To connect with MS SQL Server
import requests  # To make API requests
import json  # To handle JSON responses

# Establish connection to the MS SQL Server
def create_connection():
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=DESKTOP-J7PRN5P\\SQLEXPRESS;'  
            'DATABASE=company_data;' 
            'Trusted_Connection=yes;'
        )
        print("Database connection established.")
        return conn
    except pyodbc.Error as e:
        print(f"Error connecting to database: {e}")
        raise

# Fetch LinkedIn URLs from the database (company_data)
def fetch_company_data(cursor):
    try:
        cursor.execute("SELECT company_id, company_linkedin_url FROM company_data")
        print("Data fetched from database.")
        return cursor.fetchall()
    except pyodbc.Error as e:
        print(f"Error executing SQL query: {e}")
        raise

# Send POST request to LinkedIn bulk data API to receive additional information
def enrich_company_data(company_linkedin_url):
    url = "https://linkedin-bulk-data-scraper.p.rapidapi.com/company"  # Endpoint
    headers = {
        "x-rapidapi-key": "cf36e9f3bbmsh4d95814982bd7edp114de1jsn415c977f7620",
        "x-rapidapi-host": "linkedin-bulk-data-scraper.p.rapidapi.com",
        "Content-Type": "application/json"
    }
    payload = {"link": company_linkedin_url}  # Data sent to the API

    try:
        response = requests.post(url, json=payload, headers=headers)  # Send the request
        response.raise_for_status()  # Check the status of the response

        enriched_data = response.json()  # Parse the JSON response from the API
        company_info = enriched_data.get('data', {})  # Extract relevant data

        if not company_info:  # Check if meaningful data exists
            print(f"No meaningful data found for {company_linkedin_url}")
            return None

        # Utility function to remove whitespace
        def safe_strip(value):
            return value.strip() if value else ''

        # Dictionary to store the data extracted from the API
        relevant_data = {
            "follower_count": company_info.get('followerCount', 0),
            "tagline": safe_strip(company_info.get('tagline')),
            "industry": safe_strip(company_info.get('industry')),
            "country": safe_strip(company_info.get('headquarter', {}).get('country')),
            "city": safe_strip(company_info.get('headquarter', {}).get('city')),
            "geographic_area": safe_strip(company_info.get('headquarter', {}).get('geographicArea')),
            "postal_code": safe_strip(company_info.get('headquarter', {}).get('postalCode')),
            "company_name": safe_strip(company_info.get('companyName')),
            "url": safe_strip(company_info.get('url')),
            "website_url": safe_strip(company_info.get('websiteUrl')),
            "logo_resolution_result": safe_strip(company_info.get('logoResolutionResult')),
            "cropped_cover_image": safe_strip(company_info.get('croppedCoverImage')),
        }

        return relevant_data if any(relevant_data.values()) else None

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None
    except requests.exceptions.JSONDecodeError:
        print("Failed to decode JSON response")
        return None

# Insert enriched data into a new table in the database
def insert_enriched_data(cursor, company_id, enriched_data):
    try:
        cursor.execute(
            """
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='company_enriched_data' AND xtype='U')
            CREATE TABLE company_enriched_data (
                company_id INT PRIMARY KEY,
                follower_count INT NULL,
                tagline TEXT NULL,
                industry NVARCHAR(255) NULL,
                country NVARCHAR(100) NULL,
                city NVARCHAR(100) NULL,
                geographic_area NVARCHAR(100) NULL,
                postal_code NVARCHAR(100) NULL,
                company_name NVARCHAR(100) NULL,
                url NVARCHAR(255) NULL,
                website_url NVARCHAR(255) NULL,
                logo_resolution_result NVARCHAR(255) NULL,
                cropped_cover_image NVARCHAR(255) NULL
            )
            """
        )

        cursor.execute(
            """
            INSERT INTO company_enriched_data 
            (company_id, follower_count, tagline, industry, country, city, geographic_area, postal_code, company_name, url, website_url, logo_resolution_result, cropped_cover_image) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (company_id,
             enriched_data['follower_count'],
             enriched_data['tagline'],
             enriched_data['industry'],
             enriched_data['country'],
             enriched_data['city'],
             enriched_data['geographic_area'],
             enriched_data['postal_code'],
             enriched_data['company_name'],
             enriched_data['url'],
             enriched_data['website_url'],
             enriched_data['logo_resolution_result'],
             enriched_data['cropped_cover_image'])
        )

        print(f"Data enriched and inserted for company ID {company_id}")

    except pyodbc.Error as e:
        print(f"Error inserting data for company ID {company_id}: {e}")

# Main function to handle the workflow
def main():
    conn = create_connection()
    cursor = conn.cursor()

    try:
        company_data = fetch_company_data(cursor)

        for company_id, company_linkedin_url in company_data:
            enriched_data = enrich_company_data(company_linkedin_url)
            if enriched_data:
                insert_enriched_data(cursor, company_id, enriched_data)

        conn.commit()
        print("Enriched data inserted into the database.")

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()

    finally:
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()
