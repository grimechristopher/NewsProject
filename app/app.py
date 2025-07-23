from quart import Quart, jsonify
from config.database import db_config
from models.article import Article

# Create Quart application
app = Quart(__name__)

@app.route('/')
async def index():
    """Basic info endpoint"""
    return jsonify({
        'message': 'Quart Article API is running!',
        'status': 'healthy'
    })

@app.route('/health')
async def health_check():
    """Application health check"""
    try:
        # Test database connection
        db_healthy = db_config.test_connection()
        
        return jsonify({
            'status': 'healthy' if db_healthy else 'unhealthy',
            'database': 'connected' if db_healthy else 'disconnected',
            'table_exists': Article.table_exists()
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    # Initialize database on startup
    print("Starting Quart Article API...")
    print("Checking database connection...")
    
    if db_config.test_connection():
        print("✓ Database connection successful")
        
        # Create table if it doesn't exist
        if not Article.table_exists():
            print("Creating articles table...")
            Article.create_table()
        else:
            print("✓ Articles table exists")
    else:
        print("✗ Database connection failed - check your .env file")
    
    # Run the application
    app.run(debug=True, host='0.0.0.0', port=5000)