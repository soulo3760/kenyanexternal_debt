import requests
import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine
# from dotenv import load_dotenv
# # #load env variables
# # load_dotenv()
# # # Get environment variables
# # username = os.getenv('username')
# # password = os.getenv('password')
# # localhost = os.getenv('localhost')
# # port = os.getenv('port')
# # dbname = os.getenv('dbname')


debt_url = "https://api.worldbank.org/v2/country/KE/indicator/DT.DOD.DECT.CD?format=json"

#debt_url = 'https://api.worldbank.org/v2/country/all/indicator/DT.DOD.DECT.CD'

response = requests.get(debt_url )
data = response.json()
#print(data)
records = data[1]  # The second element contains the actual data records


# #FILTER THE DATA- year, country, debt value
# •	   - Keep relevant fields only (e.g., year, country, debt value)
# •	   - Drop or handle any missing values
# •	   - Ensure correct formatting and consistency of values


df = pd.DataFrame([{
    'country': d['country']['value'],
    'date': d['date'],
    'value': d['value']
} for d in records])

#print(df)

#transform 
#drop missing values
#print(df)
# df.isnull().sum()  # Check for missing values
# df = df.drop(columns=['country'])  # Drop the 'country' column if not needed
df = df.dropna()
#print(df)

#convert date to datetime format
df['date'] = pd.to_datetime(df['date'], format='%Y')
df = df.loc[df['date'].dt.year >= 2010]  # Filter for years >= 2000
# Rename columns for consistency
#connect to aiven postgres database
username = 'avnadmin'
password = 'AVNS_vsEbojTu2FbGFddaOxL'
localhost = 'pg-3abec650-solomioro-8aa8.f.aivencloud.com'
port = 19953
dbname = 'defaultdb'
schema = 'kenya'  # Specify the schema if needed

# # Create a connection to the PostgreSQL database
# conn = psycopg2.connect(
#     dbname=dbname,
#     user=username,
#     password=password,
#     host=localhost,
#     port=port)
#first option using psycopg2
# # Create a cursor object  to create sql table and insert data
# cursor = conn.cursor()
# # # Create a table if it does not exist
# cursor.execute('''
#     CREATE TABLE IF NOT EXISTS external_debtKe (
#         country VARCHAR(100),
#         year DATE,
#         debt_value FLOAT
#     );
# ''')
# # Commit the changes
# conn.commit()
# # Close the cursor and connection
# cursor.close()

# # Second option using SQLAlchemy
from sqlalchemy import create_engine
# Create a SQLAlchemy engine
engine = create_engine(f'postgresql://{username}:{password}@{localhost}:{port}/{dbname}')

# Save the DataFrame to the PostgreSQL database
df.to_sql('external_debtKe', con=engine, schema=schema if_exists='replace', index=False)
# Print a success message
print("Data loaded successfully to PostgreSQL database.")

#load to postgres
# CREATE TABLE external_debtKe (
#     country VARCHAR(100),
#     year DATE,
#     debt_value FLOAT
# );
#df.to_sql(f'external_debtKe', con='postgresql://{username}:{password}@pg-3abec650-solomioro-8aa8.f.aivencloud.com:19953/{dbname}', if_exists='replace', index=False)
#df.to_sql('external_debtKe', con='postgres://avnadmin:AVNS_vsEbojTu2FbGFddaOxL@pg-3abec650-solomioro-8aa8.f.aivencloud.com:19953/defaultdb?sslmode=require', if_exists='replace', index=False)

# Create a connection to the aiven PostgreSQL database

# print("Data loaded successfully to PostgreSQL database.")

