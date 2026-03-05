"""
Database session factory for connection pooling and transaction management.
Provides managed database connections with automatic cleanup.
"""

from contextlib import contextmanager
from typing import Generator, Optional
from sqlalchemy import create_engine, Engine, event
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from sqlalchemy.pool import QueuePool
import logging

from src.config import DatabaseConfig, get_config_manager


class SessionFactory:
    """
    Database session factory with connection pooling.
    Manages database connections and transactions.
    """

    def __init__(self, config: Optional[DatabaseConfig] = None):
        """
        Initialize session factory.

        Args:
            config: Database configuration
        """
        self.config = config or get_config_manager().get_database_config()
        self.logger = logging.getLogger(__name__)
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
        self._scoped_session: Optional[scoped_session] = None

    @property
    def engine(self) -> Engine:
        """Get or create database engine."""
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine

    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine with connection pooling."""
        connection_url = self._build_connection_url()

        engine = create_engine(
            connection_url,
            poolclass=QueuePool,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_timeout=self.config.pool_timeout,
            pool_recycle=self.config.pool_recycle,
            pool_pre_ping=True,  # Verify connections before using
            echo=False
        )

        # Setup connection event listeners
        self._setup_event_listeners(engine)

        self.logger.info(
            f"Database engine created: {self.config.host}:{self.config.port}/{self.config.database}"
        )

        return engine

    def _build_connection_url(self) -> str:
        """Build database connection URL."""
        if self.config.driver == "postgresql":
            driver = "postgresql+psycopg2"
        elif self.config.driver == "mysql":
            driver = "mysql+pymysql"
        elif self.config.driver == "oracle":
            driver = "oracle+cx_oracle"
        else:
            driver = self.config.driver

        return (
            f"{driver}://{self.config.username}:{self.config.password}"
            f"@{self.config.host}:{self.config.port}/{self.config.database}"
        )

    def _setup_event_listeners(self, engine: Engine) -> None:
        """Setup SQLAlchemy event listeners."""

        @event.listens_for(engine, "connect")
        def receive_connect(dbapi_conn, connection_record):
            self.logger.debug("Database connection established")

        @event.listens_for(engine, "close")
        def receive_close(dbapi_conn, connection_record):
            self.logger.debug("Database connection closed")

    @property
    def session_factory(self) -> sessionmaker:
        """Get or create session factory."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(
                bind=self.engine,
                autocommit=False,
                autoflush=False
            )
        return self._session_factory

    @property
    def scoped_session_factory(self) -> scoped_session:
        """Get or create scoped session factory for thread-safe operations."""
        if self._scoped_session is None:
            self._scoped_session = scoped_session(self.session_factory)
        return self._scoped_session

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context manager for database sessions with automatic cleanup.

        Yields:
            Database session

        Example:
            with session_factory.get_session() as session:
                result = session.query(Model).all()
        """
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(f"Session error, rolling back: {str(e)}")
            raise
        finally:
            session.close()

    @contextmanager
    def get_transactional_session(self) -> Generator[Session, None, None]:
        """
        Context manager for transactional sessions.
        Does not auto-commit; caller must explicitly commit.

        Yields:
            Database session
        """
        session = self.session_factory()
        try:
            yield session
        except Exception as e:
            session.rollback()
            self.logger.error(f"Transaction error, rolling back: {str(e)}")
            raise
        finally:
            session.close()

    def create_session(self) -> Session:
        """
        Create new session without context manager.
        Caller is responsible for closing the session.

        Returns:
            Database session
        """
        return self.session_factory()

    def get_scoped_session(self) -> Session:
        """
        Get thread-local scoped session.

        Returns:
            Scoped database session
        """
        return self.scoped_session_factory()

    def remove_scoped_session(self) -> None:
        """Remove thread-local scoped session."""
        if self._scoped_session:
            self._scoped_session.remove()

    def test_connection(self) -> bool:
        """
        Test database connection.

        Returns:
            True if connection successful
        """
        try:
            with self.get_session() as session:
                session.execute("SELECT 1")
            self.logger.info("Database connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"Database connection test failed: {str(e)}")
            return False

    def dispose(self) -> None:
        """Dispose of engine and close all connections."""
        if self._engine:
            self._engine.dispose()
            self.logger.info("Database engine disposed")

    def get_connection_info(self) -> dict:
        """
        Get connection pool information.

        Returns:
            Dictionary with pool statistics
        """
        if self._engine:
            pool = self._engine.pool
            return {
                'pool_size': pool.size(),
                'checked_out': pool.checkedout(),
                'overflow': pool.overflow(),
                'total_connections': pool.size() + pool.overflow()
            }
        return {}


# Singleton instance
_session_factory: Optional[SessionFactory] = None


def get_session_factory(config: Optional[DatabaseConfig] = None) -> SessionFactory:
    """
    Get singleton session factory instance.

    Args:
        config: Database configuration

    Returns:
        SessionFactory instance
    """
    global _session_factory
    if _session_factory is None:
        _session_factory = SessionFactory(config)
    return _session_factory