import requests
import pandas as pd
from datetime import datetime
import json
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import execute_batch

# Load API key from .env file
load_dotenv()

def get_transaction_data():
    url = os.getenv('API_URL') 
    # Set up API request headers
    headers = {
        'x-api-key': os.getenv('API_KEY'),
        'Content-Type': 'application/json'
    }
    
    # Define date range for data fetch
    payload = {
        "start_date": "2023-01-01",
        "end_date": "2023-01-31"
    }

    try:
        # Make API request and convert to DataFrame
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        df = pd.DataFrame(response.json())
        print("Available columns:", df.columns.tolist())
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def clean_data(df):
    if df is None or df.empty:
        print("No data to clean")
        return None

    print("\n=== Data Cleaning Report ===")
    print(f"Original row count: {len(df)}")
    
    # Create copy to avoid modifying original data
    clean_df = df.copy()
    
    print("\nMissing values before cleaning:")
    print(clean_df.isnull().sum())
    
    # Check for required columns
    available_columns = clean_df.columns.tolist()
    critical_columns = [col for col in ['transaction_date'] if col in available_columns]
    
    # Remove rows with missing critical data
    if critical_columns:
        clean_df.dropna(subset=critical_columns, inplace=True)
    
    # Standardize date format
    if 'transaction_date' in clean_df.columns:
        try:
            clean_df['transaction_date'] = pd.to_datetime(clean_df['transaction_date'])
            clean_df['transaction_date'] = clean_df['transaction_date'].dt.strftime('%Y-%m-%d')
        except Exception as e:
            print(f"Error converting dates: {e}")
    
    # Remove duplicate records
    duplicates_count = clean_df.duplicated().sum()
    clean_df.drop_duplicates(inplace=True)
    print(f"\nDuplicates removed: {duplicates_count}")
    
    # Clean transaction negative amounts
    if 'transaction_amount' in clean_df.columns:
        clean_df['transaction_amount'] = pd.to_numeric(clean_df['transaction_amount'], errors='coerce')
        negative_count = len(clean_df[clean_df['transaction_amount'] < 0])
        clean_df = clean_df[clean_df['transaction_amount'] >= 0]
        print(f"Negative transactions removed: {negative_count}")
    
    print("\nMissing values after cleaning:")
    print(clean_df.isnull().sum())
    print(f"\nFinal row count: {len(clean_df)}")
    
    return clean_df

def enrich_data(df):
    if df is None or df.empty:
        print("No data to enrich")
        return None

    enriched_df = df.copy()
    
    print("\n=== Enrichment Process ===")
    #print("Columns before enrichment:", enriched_df.columns.tolist())
    
    # Function to categorize transaction amounts
    def categorize_amount(amount):
        try:
            amount = float(amount)
            if amount < 50:
                return 'low'
            elif amount <= 200:
                return 'medium'
            else:
                return 'high'
        except (ValueError, TypeError):
            return 'unknown'

    # Process and categorize transaction amounts
    if 'transaction_amount' in enriched_df.columns:
        
        # Convert amounts to numeric and create categories
        enriched_df['transaction_amount'] = pd.to_numeric(enriched_df['transaction_amount'], errors='coerce')
        enriched_df['amount_category'] = enriched_df['transaction_amount'].apply(categorize_amount)
        print("\nAmount categories created:", enriched_df['amount_category'].value_counts())

        # Calculate customer total transactions
        customer_totals = enriched_df.groupby('customer_id')['transaction_amount'].sum().reset_index()
        customer_totals.columns = ['customer_id', 'total_customer_transactions']
        enriched_df = enriched_df.merge(customer_totals, on='customer_id', how='left')

    print("\nColumns after enrichment:", enriched_df.columns.tolist())
    return enriched_df

def display_data(df):
    if df is None or df.empty:
        print("No data to display")
        return

    # Configure display settings
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    # Display data overview
    print("\n=== Enriched Dataset Overview ===")
    print(f"Total rows: {len(df)}")
    
    
    print("\n=== Sample of Enriched Data ===")
    print(df.head(10))
    
    


def get_db_connection():
    return psycopg2.connect(
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )


def save_to_database(enriched_df):
    conn = None
    try:
        # Convert DataFrame to use Python native types
        enriched_df = enriched_df.astype({
            'customer_id': str,
            'product_id': str,
            'product_category': str,
            'transaction_date': str,
            'transaction_amount': float,
            'transaction_type': str,
            'spend_category': str,
            'amount_category': str
        })

        conn = get_db_connection()
        cur = conn.cursor()
        
        print("\n=== Starting Database Insert Process ===")
        
        #Insert into customers table
        customer_query = """
        INSERT INTO customers (customer_id) 
        VALUES (%s) 
        ON CONFLICT (customer_id) DO NOTHING
        """
        customer_data = [(str(customer_id),) for customer_id in enriched_df['customer_id'].unique()]
        execute_batch(cur, customer_query, customer_data)
        print("✅ Customers data inserted")

        #Insert into products table
        product_query = """
        INSERT INTO products (product_id, product_category) 
        VALUES (%s, %s) 
        ON CONFLICT (product_id) DO NOTHING
        """
        product_data = [(str(row['product_id']), str(row['product_category'])) 
                       for _, row in enriched_df[['product_id', 'product_category']].drop_duplicates().iterrows()]
        execute_batch(cur, product_query, product_data)
        print("✅ Products data inserted")

        # Insert into transaction_types table
        transaction_type_query = """
        INSERT INTO transaction_types (transaction_type_name) 
        VALUES (%s) 
        ON CONFLICT (transaction_type_name) DO NOTHING
        """
        transaction_type_data = [(str(type_),) for type_ in enriched_df['transaction_type'].unique()]
        execute_batch(cur, transaction_type_query, transaction_type_data)
        print("✅ Transaction types inserted")

        # Insert into spend_categories table
        spend_category_query = """
        INSERT INTO spend_categories (spend_category_name) 
        VALUES (%s) 
        ON CONFLICT (spend_category_name) DO NOTHING
        """
        spend_category_data = [(str(cat),) for cat in enriched_df['spend_category'].unique()]
        execute_batch(cur, spend_category_query, spend_category_data)
        print("✅ Spend categories inserted")

        #Insert into amount_categories table
        amount_categories = {
            'low': (0, 50),
            'medium': (50, 200),
            'high': (200, 999999.99)
        }
        for category, (min_amt, max_amt) in amount_categories.items():
            cur.execute("""
                INSERT INTO amount_categories (category_name, min_amount, max_amount)
                VALUES (%s, %s, %s)
                ON CONFLICT (category_name) DO NOTHING
            """, (category, float(min_amt), float(max_amt)))
        print("✅ Amount categories inserted")

        #Insert into transactions table
        transaction_query = """
        INSERT INTO transactions 
        (customer_id, product_id, transaction_date, transaction_amount, 
         transaction_type_id, spend_category_id, amount_category_id)
        VALUES (%s, %s, %s, %s, 
        (SELECT transaction_type_id FROM transaction_types WHERE transaction_type_name = %s),
        (SELECT spend_category_id FROM spend_categories WHERE spend_category_name = %s),
        (SELECT amount_category_id FROM amount_categories WHERE category_name = %s))
        """
        
        transaction_data = [
            (
                str(row['customer_id']),
                str(row['product_id']),
                str(row['transaction_date']),
                float(row['transaction_amount']),
                str(row['transaction_type']),
                str(row['spend_category']),
                str(row['amount_category'])
            )
            for _, row in enriched_df.iterrows()
        ]
        execute_batch(cur, transaction_query, transaction_data)
        print("✅ Transactions data inserted")

        conn.commit()
        
        print("\n=== Insert Summary ===")
        print(f"Customers processed: {len(customer_data)}")
        print(f"Products processed: {len(product_data)}")
        print(f"Transaction types processed: {len(transaction_type_data)}")
        print(f"Spend categories processed: {len(spend_category_data)}")
        print(f"Amount categories processed: {len(amount_categories)}")
        print(f"Transactions processed: {len(transaction_data)}")
        print("\n✅ Database Insert Process Completed Successfully")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"❌ Error inserting data: {e}")
        print("\nDataFrame Info:")
        print(enriched_df.info())
        print("\nDataFrame Data Types:")
        print(enriched_df.dtypes)
    
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed")
            

def main():
    # Test database connection first
    if not get_db_connection():
        print("Exiting due to database connection failure")
        return

  #fetch raw data
    raw_df = get_transaction_data()
    
    #clean data
    if raw_df is not None:
        cleaned_df = clean_data(raw_df)
        
        #Enchrinch data
        if cleaned_df is not None:
            enriched_df = enrich_data(cleaned_df)
            
            #Display and save results
            if enriched_df is not None:
                display_data(enriched_df)
                
            
                
                print("\nSaving data to database...")
                save_to_database(enriched_df)
                print("Process completed!")


if __name__ == "__main__":
    main()



