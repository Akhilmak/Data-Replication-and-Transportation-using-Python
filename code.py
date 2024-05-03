import mysql.connector
import pandas as pd
def replicate_table():
    try:
        source_host = input("Enter source host: ")
        source_port = int(input("Enter source port: "))
        source_user = input("Enter source user: ")
        source_password = input("Enter source password: ")
        source_database = input("Enter source database: ")
        source_connection = mysql.connector.connect(
            host=source_host,
            port=source_port,
            user=source_user,
            password=source_password,
            database=source_database
        )
        if source_connection.is_connected():
            print(f"Connected to the source database {source_database}")
            source_cursor = source_connection.cursor()
            source_cursor.execute("SHOW TABLES")
            tables = source_cursor.fetchall()
            print("Tables in the source database:")
            for table in tables:
                print(table[0])
            table_to_replicate = input("Enter the name of the table to replicate: ")
            source_cursor.execute(f"SHOW CREATE TABLE {table_to_replicate}")
            table_structure = source_cursor.fetchone()[1]
            duplicate_table_name = f"{table_to_replicate}_duplicate"
            source_cursor.execute(f"CREATE TABLE IF NOT EXISTS {duplicate_table_name} LIKE {table_to_replicate}")
            source_connection.commit()
            print(f"Created a duplicate table {duplicate_table_name}")
            source_cursor.execute(f"INSERT INTO {duplicate_table_name} SELECT * FROM {table_to_replicate}")
            source_connection.commit()
            source_cursor = source_connection.cursor()
            source_cursor.execute(f"SELECT * FROM {duplicate_table_name}")
            rows = source_cursor.fetchall()
            column_names = [col[0] for col in source_cursor.description]
            df = pd.DataFrame(rows, columns=column_names)
            dest_host = input("Enter destination host: ")
            dest_port = int(input("Enter destination port: "))
            dest_user = input("Enter destination user: ")
            dest_password = input("Enter destination password: ")
            dest_database = input("Enter destination database: ")
            dest_connection = mysql.connector.connect(
                host=dest_host,
                port=dest_port,
                user=dest_user,
                password=dest_password,
                database=dest_database
            )
            if dest_connection.is_connected():
                print(f"Connected to the destination database {dest_database}")
                dest_cursor = dest_connection.cursor()
                try:
                    dest_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {dest_database}")
                    dest_connection.database = dest_database
                    print(f"Switched to the destination database {dest_database}")
                    dest_cursor.execute(f"SHOW TABLES LIKE '{table_to_replicate}'")
                    existing_table = dest_cursor.fetchone()
                    if existing_table:
                        overwrite = input(f"The table '{table_to_replicate}' already exists in the destination database. Do you want to overwrite it? (yes/no): ")
                        if overwrite.lower() == 'no':
                            new_table_name = input("Enter a new name for the table: ")
                            dest_cursor.execute(f"CREATE TABLE IF NOT EXISTS {new_table_name} LIKE {table_to_replicate}")
                            print(f"Created the new table {new_table_name}")
                        else:
                            dest_cursor.execute(f"DROP TABLE IF EXISTS {table_to_replicate}")
                            dest_cursor.execute(table_structure.replace(table_to_replicate, table_to_replicate))
                            print(f"Overwritten the existing table {table_to_replicate}")
                    else:
                        dest_cursor.execute(table_structure.replace(table_to_replicate, table_to_replicate))
                        print(f"Created the new table {table_to_replicate}")
                    for index, row in df.iterrows():
                        query = f"INSERT INTO {table_to_replicate} VALUES ({', '.join(['%s'] * len(row))})"
                        dest_cursor.execute(query, tuple(row))
                    dest_connection.commit()
                    print(f"Transferred all the data from {table_to_replicate} to {table_to_replicate}")
                except mysql.connector.Error as err:
                    print(err)
                finally:
                    dest_cursor.close()
        else:
            print("Failed to connect to source database")
    except mysql.connector.Error as err:
        print("Error while connecting to MySQL:", err)
    finally:
        if source_connection.is_connected():
            source_connection.close()
            print("Source MySQL connection is closed")
        if 'dest_connection' in locals() and dest_connection.is_connected():
            dest_connection.close()
            print("Destination MySQL connection is closed")
replicate_table()
