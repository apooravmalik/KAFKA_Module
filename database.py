import os
import urllib
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import logging
from config import DB_CONFIG

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


DB_SCHEMA = "dbo"

# Function to create connection string for sop-manage
def create_connection_string():
    """Create a properly formatted connection string for MS SQL Server"""
    params = urllib.parse.quote_plus(
        f"DRIVER={{{DB_CONFIG['driver']}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        f"TrustServerCertificate={'yes' if DB_CONFIG['trust_cert'].lower() == 'yes' else 'no'};"
        f"Timeout=60;"
    )
    return f"mssql+pyodbc:///?odbc_connect={params}"


# Create engine for sop-manage
engine = create_engine(
    create_connection_string(),
    echo=True,  # Set to False in production
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=3600,
)

# Session factory for sop-manage
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False
)

# Dependency function for sop-manage database session
def get_db():
    """Database session dependency for sop-manage"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Test connection for sop-manage database
def test_connection_sop_manage():
    """Test connection to sop-manage database"""
    try:
        with engine.connect() as connection:
            logger.info("Successfully connected to sop-manage database!")
            return True
    except Exception as e:
        logger.error(f"Error connecting to sop-manage database: {e}")
        return False

