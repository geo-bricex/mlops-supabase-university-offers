import os
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv

load_dotenv()

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("db_session")

# Connection String Construction
# In docker-compose, postgres runs on port 5432 (internal) but mapped to 54322 (external).
# When running python locally (outside docker), use 54322.
# When running inside docker, use 5432 and host 'db'.

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "54322") 
DB_USER = os.getenv("DB_USER", "supabase_admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "your-super-secret-and-long-postgres-password")
DB_NAME = os.getenv("DB_NAME", "postgres")

# Construct the connection string.
# Fallback for full connection string provided in env
DATABASE_URL = os.getenv("DB_CONNECTION_STRING")
if not DATABASE_URL:
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
ScopedSession = scoped_session(SessionLocal)

Base = declarative_base()

@contextmanager
def get_db_session():
    """Provide a transactional scope around a series of operations."""
    session = ScopedSession()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Session rollback due to error: {e}")
        raise
    finally:
        session.close()

def init_db():
    # Typically we use alembic or init.sql, but this can be used 
    # if we define models in python and want to create them.
    # For this project, we primarily rely on sql/init.sql.
    pass
