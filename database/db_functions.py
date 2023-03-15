import psycopg2
import pandas as pd
import os
import datetime

class Database:

    def __init__(self, dbname='filiptomanka', user='postgres', port='5433'):
        self.dbname = dbname
        self.user = user
        self.port = port
        print('Database object was initialized.')


    def connect(self):
        self.conn = psycopg2.connect(
            dbname = self.dbname,
            user= self.user,
            port = self.port
            )
        self.cur = self.conn.cursor()
        print('Connected to database.')


    def disconnect(self):
        self.cur.close()
        self.conn.close()
        print('Disconnected from database.')


    def get_table_list(self):

        self.connect()
        
        self.cur.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public'")
        table_names = self.cur.fetchall()
        print('Database contains following tables:')
        for table_name in table_names:
            print(table_name[0])

        self.disconnect()


    def create_table(self, table_name, **columns):
    
        self.connect()

        self.cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');")
        table_exists = self.cur.fetchone()[0]
        if table_exists:
            print(f"Table {table_name} already exists.")
            self.disconnect()
            return

        sql = f"CREATE TABLE {table_name} ("
        for col_name, col_type in columns.items():
            sql += f"{col_name} {col_type}, "
        sql = sql[:-2] # remove the trailing comma and space
        sql += ");"

        self.cur.execute(sql)
        self.conn.commit()
        print(f"Table {table_name} created successfully.")

        self.disconnect()



    def drop_table(self, table_name):

        self.connect()

        try:
            self.cur.execute(f"DROP TABLE {table_name}")
            print(f'Dropped table {table_name}')
            self.conn.commit()
        except:
            print(f'Table {table_name} doesnt exist.')

        self.disconnect()



    def back_up_the_database(self):

        self.connect()
        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
        table_names = [row[0] for row in self.cur.fetchall()]

        # Create a folder for the export files named after the current date and time
        export_folder = '/Users/filiptomanka/Programming/algo_trading/database/backup/' + datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        os.makedirs(export_folder, exist_ok=True)

        # Loop through the table names and export each table to a CSV file
        for table_name in table_names:
            query = f"SELECT * FROM {table_name}"
            self.cur.execute(query)
            rows = self.cur.fetchall()
            column_names = [desc[0] for desc in self.cur.description]
            df = pd.DataFrame(rows, columns=column_names)
            export_path = os.path.join(export_folder, f"{table_name}.csv")
            df.to_csv(export_path, index=False)
            print(f'Backing up table {table_name}')

        self.disconnect()


    
    # def insert_into_table(self, table_name, data_dict):

    #     self.connect()

    #     # Construct the INSERT query
    #     columns = ", ".join(data_dict.keys())
    #     values = ", ".join([f"%({key})s" for key in data.keys()])
    #     query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

    #     # Execute the INSERT query with the data provided
    
    #     self.cur.execute(query, data)
    #     self.conn.commit()
    #     print(f'Inserted data into table {table_name}')

    #     # except:
    #     #     print(f'There was a problem while inserting data into table {table_name}')

    #     self.disconnect()


    def insert_into_table(self, table_name, data_dict):
        self.connect()
        
        # Get the list of column names from the dictionary keys
        columns = ", ".join(data_dict.keys())
        
        # Loop through the values and construct the INSERT query for each row
        for i in range(len(data_dict[list(data_dict.keys())[0]])):
            values = []
            for key in data_dict.keys():
                values.append(data_dict[key][i])
            values_str = ", ".join([f"'{val}'" for val in values])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_str})"
            
            # Execute the INSERT query with the current row of values
            self.cur.execute(query)
            self.conn.commit()
        
        print(f"Inserted data into table {table_name}")
        self.disconnect()


    
    def download_table(self, table_name):
        self.connect()
        query = f"SELECT * FROM {table_name}"
        try:
            self.cur.execute(query)
            rows = self.cur.fetchall()
            column_names = [desc[0] for desc in self.cur.description]
            df = pd.DataFrame(rows, columns=column_names)
            print(f'Created df out of table {table_name}')
            self.disconnect
            return df
        except:
            self.disconnect
            print(f'There was an error downloading table {table_name}')



    def remove_duplicates(self, table_name, unique_column):
        self.connect()
        self.cur.execute(f"SELECT table_schema FROM information_schema.tables WHERE table_name = '{table_name}'")
        schema_name = self.cur.fetchone()[0]
        self.cur.execute(f"CREATE TABLE {schema_name}.{table_name}_temp AS SELECT DISTINCT ON ({unique_column}) * FROM {schema_name}.{table_name}")
        self.cur.execute(f"DROP TABLE {schema_name}.{table_name}")
        self.cur.execute(f"ALTER TABLE {schema_name}.{table_name}_temp RENAME TO {table_name}")
        self.conn.commit()
        self.disconnect()


    def create_dataframe_from_table(self, table_name):
        self.connect()
        self.cur.execute(f"SELECT * FROM {table_name}")
        data = self.cur.fetchall()
        column_names = [desc[0] for desc in self.cur.description]
        df = pd.DataFrame(data, columns=column_names)
        self.disconnect()
        return df
    
    def fetch_todays_data(self, table_name, published_at_column):

        self.connect()

        query = f"SELECT * FROM {table_name} WHERE {published_at_column}::date = %s"
        today = datetime.date.today()
        self.cur.execute(query, (today,))

        data = self.cur.fetchall()
        column_names = [desc[0] for desc in self.cur.description]
        df = pd.DataFrame(data, columns=column_names)
        self.disconnect()
        return df
