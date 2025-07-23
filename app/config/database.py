import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class DatabaseConfig:
    """Database configuration and connection management"""
    
    def __init__(self):
        self.host = os.getenv('DB_HOST')
        self.port = os.getenv('DB_PORT')
        self.name = os.getenv('DB_NAME')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"
    
    def get_connection(self):
        """Get a database connection"""
        try:
            return psycopg2.connect(
                self.url,
                cursor_factory=RealDictCursor  # This returns dict-like rows
            )
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise

    def test_connection(self):
        """Test the database connection"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1;")
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            return result is not None
        except Exception as e:
            print(f"Database connection test failed: {e}")
            return False

# Global database config instance
db_config = DatabaseConfig()