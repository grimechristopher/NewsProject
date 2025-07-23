from datetime import datetime
from typing import List, Optional, Dict, Any
from config.database import db_config

class Article:
    """Article model for database operations"""
    
    def __init__(self, id: Optional[int] = None, title: str = "", 
                 raw_content: str = "", direct_link: str = "", 
                 created_at: Optional[datetime] = None, 
                 updated_at: Optional[datetime] = None):
        self.id = id
        self.title = title
        self.raw_content = raw_content
        self.direct_link = direct_link
        self.created_at = created_at
        self.updated_at = updated_at

    def to_dict(self) -> Dict[str, Any]:
        """Convert article instance to dictionary"""
        return {
            'id': self.id,
            'title': self.title,
            'raw_content': self.raw_content,
            'direct_link': self.direct_link,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Article':
        """Create article instance from dictionary"""
        return cls(
            id=data.get('id'),
            title=data.get('title', ''),
            raw_content=data.get('raw_content', ''),
            direct_link=data.get('direct_link', ''),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at')
        )

    @staticmethod
    def create_table() -> bool:
        """Create the articles table with specified columns"""
        
        # Drop table if exists (optional - remove if you don't want to overwrite)
        drop_query = "DROP TABLE IF EXISTS articles;"
        
        # Create table query
        create_query = """
        CREATE TABLE articles (
            id SERIAL PRIMARY KEY,
            title VARCHAR(500) NOT NULL,
            raw_content TEXT,
            direct_link VARCHAR(1000) UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Create index for better performance on common searches
        index_query = """
        CREATE INDEX IF NOT EXISTS idx_articles_title ON articles(title);
        CREATE INDEX IF NOT EXISTS idx_articles_direct_link ON articles(direct_link);
        """
        
        try:
            conn = db_config.get_connection()
            cursor = conn.cursor()
            
            print("Creating articles table...")
            
            # Execute drop (optional)
            cursor.execute(drop_query)
            
            # Execute create
            cursor.execute(create_query)
            print("Articles table created successfully!")
            
            # Create indexes
            cursor.execute(index_query)
            print("Indexes created successfully!")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Verify table creation
            if Article.table_exists():
                print("Table 'articles' confirmed in database")
                return True
            else:
                print("Warning: Could not verify table creation")
                return False
                
        except Exception as e:
            print(f"Error creating table: {e}")
            return False

    @staticmethod
    def table_exists() -> bool:
        """Check if articles table exists"""
        try:
            conn = db_config.get_connection()
            cursor = conn.cursor()
            
            query = "SELECT table_name FROM information_schema.tables WHERE table_name='articles';"
            cursor.execute(query)
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return result is not None
        except Exception as e:
            print(f"Error checking table existence: {e}")
            return False

    @staticmethod
    def get_table_structure() -> List[Dict[str, Any]]:
        """Get the structure of the articles table"""
        query = """
        SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = 'articles'
        ORDER BY ordinal_position;
        """
        
        try:
            conn = db_config.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            
            return [dict(row) for row in results]
        except Exception as e:
            print(f"Error getting table structure: {e}")
            return []

    def save(self) -> Optional[int]:
        """Insert or update article in the database"""
        if self.id:
            return self._update()
        else:
            return self._insert()
    
    def _insert(self) -> Optional[int]:
        """Insert a new article into the table"""
        query = """
        INSERT INTO articles (title, raw_content, direct_link) 
        VALUES (%s, %s, %s) RETURNING id, created_at, updated_at;
        """
        
        try:
            conn = db_config.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(query, (self.title, self.raw_content, self.direct_link))
            result = cursor.fetchone()
            
            self.id = result['id']
            self.created_at = result['created_at']
            self.updated_at = result['updated_at']
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"Article inserted successfully with ID: {self.id}")
            return self.id
        except Exception as e:
            print(f"Error inserting article: {e}")
            return None
    
    def _update(self) -> Optional[int]:
        """Update existing article"""
        query = """
        UPDATE articles 
        SET title = %s, raw_content = %s, direct_link = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        RETURNING updated_at;
        """
        
        try:
            conn = db_config.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(query, (self.title, self.raw_content, self.direct_link, self.id))
            result = cursor.fetchone()
            
            if result:
                self.updated_at = result['updated_at']
                conn.commit()
                cursor.close()
                conn.close()
                print(f"Article {self.id} updated successfully")
                return self.id
            else:
                cursor.close()
                conn.close()
                print(f"Article {self.id} not found")
                return None
                
        except Exception as e:
            print(f"Error updating article: {e}")
            return None

    @staticmethod
    def get_all() -> List['Article']:
        """Retrieve all articles from the table"""
        query = "SELECT * FROM articles ORDER BY created_at DESC;"
        
        try:
            conn = db_config.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            
            return [Article.from_dict(dict(row)) for row in results]
        except Exception as e:
            print(f"Error retrieving articles: {e}")
            return []

    @staticmethod
    def get_by_id(article_id: int) -> Optional['Article']:
        """Retrieve article by ID"""
        query = "SELECT * FROM articles WHERE id = %s;"
        
        try:
            conn = db_config.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (article_id,))
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result:
                return Article.from_dict(dict(result))
            return None
        except Exception as e:
            print(f"Error retrieving article: {e}")
            return None

    @staticmethod
    def get_by_link(direct_link: str) -> Optional['Article']:
        """Retrieve article by direct link"""
        query = "SELECT * FROM articles WHERE direct_link = %s;"
        
        try:
            conn = db_config.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (direct_link,))
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result:
                return Article.from_dict(dict(result))
            return None
        except Exception as e:
            print(f"Error retrieving article: {e}")
            return None

    def delete(self) -> bool:
        """Delete this article from database"""
        if not self.id:
            return False
            
        query = "DELETE FROM articles WHERE id = %s;"
        
        try:
            conn = db_config.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (self.id,))
            deleted_rows = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            
            if deleted_rows > 0:
                print(f"Article {self.id} deleted successfully")
                return True
            else:
                print(f"Article {self.id} not found")
                return False
                
        except Exception as e:
            print(f"Error deleting article: {e}")
            return False

    @staticmethod
    def search_by_title(title_query: str) -> List['Article']:
        """Search articles by title (case-insensitive partial match)"""
        query = "SELECT * FROM articles WHERE title ILIKE %s ORDER BY created_at DESC;"
        
        try:
            conn = db_config.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (f'%{title_query}%',))
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            
            return [Article.from_dict(dict(row)) for row in results]
        except Exception as e:
            print(f"Error searching articles: {e}")
            return []