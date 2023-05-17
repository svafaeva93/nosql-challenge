import csv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

connection = psycopg2.connect(
    dbname="(name of database)",
    user="postgres",
    password="",
    host="localhost",
    port="5432"
)

csv_file = r'/Users/shakhnoza/Documents/GitHub/Covid19AnalysisCanada/Output/Covid19_data_Output2.csv'
table = 'covid_output'

try:
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        columns = next(reader)
        insert_query = sql.SQL('INSERT INTO {} VALUES %s').format(
            sql.Identifier(table)
        )
    
        cursor = connection.cursor()
        execute_values(cursor, insert_query, [row for row in reader])
        connection.commit()
        print(f"Data loaded into {table} successfully!")

except Exception as e:
    print("Error loading data:", e)
    connection.rollback()

finally:
    connection.close()
