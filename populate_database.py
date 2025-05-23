import mysql.connector
import os
import sys

def get_database_connection():
    """Create and return a database connection using environment variables."""
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', 3306)),
            user=os.getenv('DB_USER', 'testuser'),
            password=os.getenv('DB_PASSWORD', 'testpass'),
            database=os.getenv('DB_NAME', 'testdb')
        )
        return connection
    except mysql.connector.Error as error:
        print(f"Error connecting to MySQL: {error}")
        sys.exit(1)

def populate_database():
    """Populate the database with sample data."""
    connection = get_database_connection()
    cursor = connection.cursor()
    
    try:
        # Example: Insert sample data
        sample_data = [
            ('John Doe', 'john@example.com'),
            ('Jane Smith', 'jane@example.com'),
            ('Bob Johnson', 'bob@example.com')
        ]
        
        insert_query = "INSERT INTO users (name, email) VALUES (%s, %s)"
        cursor.executemany(insert_query, sample_data)
        
        connection.commit()
        print(f"Successfully inserted {cursor.rowcount} records")
        
        # Verify the insertion
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        print(f"Total records in users table: {count}")
        
    except mysql.connector.Error as error:
        print(f"Error populating database: {error}")
        connection.rollback()
        sys.exit(1)
    
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    print("Starting database population...")
    populate_database()
    print("Database population completed!")
