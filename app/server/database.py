#!/usr/bin/env python3
# Database module for the server

import os
import sqlite3
import datetime
import pandas as pd
import threading
import queue
import logging
import contextlib
import json

logger = logging.getLogger(__name__)
class Database:
    """Thread-safe database using a connection pool for storing client info and query history"""
    
    def __init__(self, db_path='app/server/server_data.db', pool_size=10):
        """Initialize database pool and create tables if they don't exist"""
        self.db_path = db_path
        self.pool_size = pool_size
        self._pool = queue.Queue(maxsize=pool_size)
        self._closed = False # Flag to indicate pool shutdown

        # Lock for initializing/closing the pool safely
        self._init_lock = threading.Lock()

        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self._initialize_pool()

        # Perform database migration if needed (using a pooled connection)
        self.migrate_database()
        
        # Ensure tables exist (using a pooled connection)
        try:
            conn = self.get_connection()
            if conn:
                self.create_tables(conn)
        except Exception as e:
            logger.error(f"Error creating tables during init: {e}", exc_info=True)
            raise
        finally:
            if conn:
                self.return_connection(conn)

    def _initialize_pool(self):
        """Populate the connection pool."""
        with self._init_lock:
            if self._closed:
                 raise RuntimeError("Database pool is closed.")
            logger.info(f"Initializing database connection pool (size {self.pool_size})...")
            for _ in range(self.pool_size):
                try:
                    # Connections in pool must allow sharing across threads
                    conn = sqlite3.connect(self.db_path, check_same_thread=False)
                    conn.execute("PRAGMA foreign_keys = ON")
                    conn.row_factory = sqlite3.Row
                    self._pool.put(conn)
                except sqlite3.Error as e:
                    logger.error(f"Failed to create connection for pool: {e}", exc_info=True)
                    # Handle error appropriately - maybe raise, maybe try fewer connections?
                    raise RuntimeError(f"Failed to initialize database pool: {e}") from e
            logger.info(f"Database connection pool initialized with {self._pool.qsize()} connections.")

    def migrate_database(self):
        """Perform necessary database migrations"""
        # Needs careful handling as pool might not be fully ready
        conn = None
        try:
            # Temporarily get a raw connection for migration check before pool might be used
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if sessions table exists and has the old schema
            cursor.execute("PRAGMA table_info(sessions)")
            columns = cursor.fetchall()
            
            column_names = [col[1] for col in columns] if columns else []
            
            # If sessions table exists but doesn't have 'address' column
            if columns and 'address' not in column_names:
                # Drop the old sessions table as we're changing the structure
                # (another option would be to ALTER TABLE, but this is simpler for a new system)
                cursor.execute("DROP TABLE IF EXISTS sessions")
                conn.commit()
                print("Migrated database: dropped old sessions table")
            
            conn.close()
        except Exception as e:
            logger.error(f"Error during database migration check: {e}")
            if conn:
                try: conn.close() # Ensure temporary connection is closed on error
                except: pass
        # Note: Actual table creation uses the pool via create_tables called from __init__

    def get_connection(self):
        """Get a connection from the pool.
        Blocks until a connection is available, with a timeout.
        """
        if self._closed:
            raise RuntimeError("Database pool is closed.")
        try:
            # Wait up to 10 seconds for a connection
            conn = self._pool.get(block=True, timeout=10)
            logger.debug(f"Acquired DB connection {id(conn)} from pool (pool size: {self._pool.qsize()})")
            return conn
        except queue.Empty:
            logger.error("Timeout waiting for database connection from pool.")
            # Depending on policy, could raise error or return None
            raise TimeoutError("Timeout waiting for database connection from pool.")

    def return_connection(self, conn):
        """Return a connection to the pool."""
        if conn is None:
            return
        if self._closed:
            # If pool is closed, just close the connection we got
            try:
                conn.close()
            except Exception as e:
                 logger.warning(f"Error closing connection {id(conn)} after pool shutdown: {e}", exc_info=True)
            return
        try:
            # Use block=False with check to avoid waiting if pool is somehow full
            # (shouldn't happen with correct usage but safer)
            if not self._pool.full():
                 self._pool.put(conn, block=False)
                 logger.debug(f"Returned DB connection {id(conn)} to pool (pool size: {self._pool.qsize()})")
            else:
                 logger.warning(f"Attempted to return connection {id(conn)} to a full pool. Closing instead.")
                 try:
                      conn.close()
                 except Exception as e:
                      logger.error(f"Error closing connection {id(conn)} that couldn't be returned to full pool: {e}", exc_info=True)
        except Exception as e:
             logger.error(f"Error returning connection {id(conn)} to pool: {e}. Closing connection.", exc_info=True)
             # Ensure connection is closed if putting back failed
             try:
                  conn.close()
             except Exception as close_e:
                  logger.error(f"Error closing connection {id(conn)} after failing to return to pool: {close_e}", exc_info=True)

    def close_all_connections(self):
        """Close all connections in the pool and shut down the pool."""
        with self._init_lock:
            if self._closed:
                return # Already closed
            logger.info(f"Closing all database connections in the pool ({self._pool.qsize()} connections estimated)...")
            self._closed = True
            closed_count = 0
            while not self._pool.empty():
                try:
                    conn = self._pool.get_nowait()
                    conn.close()
                    closed_count += 1
                except queue.Empty:
                    break # Pool is empty
                except Exception as e:
                    logger.error(f"Error closing connection during pool shutdown: {e}", exc_info=True)
            logger.info(f"Database connection pool shutdown complete. Closed {closed_count} connections.")

    @contextlib.contextmanager
    def get_connection_context(self):
         """Context manager for getting and returning a connection."""
         conn = None
         try:
              conn = self.get_connection()
              yield conn
         finally:
              if conn:
                   self.return_connection(conn)

    def create_tables(self, conn):
        """Create tables if they don't exist using a provided connection."""
        # This method now expects a connection to be passed in
        # It's called from __init__ which handles getting/returning the connection
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            nickname TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            password TEXT NOT NULL,
            registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            address TEXT NOT NULL,
            start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            end_time TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients (id)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            session_id INTEGER NOT NULL,
            query_type TEXT NOT NULL,
            parameters TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients (id),
            FOREIGN KEY (session_id) REFERENCES sessions (id)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_type TEXT NOT NULL,
            sender_id INTEGER NOT NULL,
            recipient_type TEXT NOT NULL,
            recipient_id INTEGER NOT NULL,
            message TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            read INTEGER DEFAULT 0
        )
        ''')
        
        conn.commit()
    
    def register_client(self, name, nickname, email, password):
        """Register a new client"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT * FROM clients WHERE nickname = ? OR email = ?", (nickname, email))
                if cursor.fetchone():
                    return False
                cursor.execute(
                    "INSERT INTO clients (name, nickname, email, password) VALUES (?, ?, ?, ?)",
                    (name, nickname, email, password)
                )
                conn.commit()
                return True
            except sqlite3.Error as e:
                logger.error(f"DB Error registering client {nickname}: {e}", exc_info=True)
                conn.rollback()
                raise  # Re-raise the database error
    
    def check_login(self, email, password):
        """Check login credentials and return client info if valid"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM clients WHERE email = ? AND password = ?", (email, password))
            row = cursor.fetchone()
            if row:
                # Convert row to dict before connection is returned
                return dict(row)
            else:
                return None
    
    def start_session(self, client_id, address):
        """Start a new session for a client and return its ID and start time."""
        start_time = datetime.datetime.now(datetime.timezone.utc).isoformat() # Record start time
        session_id = None
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "INSERT INTO sessions (client_id, address, start_time) VALUES (?, ?, ?)",
                    (client_id, address, start_time)
                )
                session_id = cursor.lastrowid
                conn.commit()
            except sqlite3.Error as e:
                 logger.error(f"DB Error starting session for client {client_id}: {e}", exc_info=True)
                 conn.rollback()
                 raise # Re-raise
        
        if session_id:
             return {'id': session_id, 'start_time': start_time}
        else:
             # Handle case where insert somehow failed to return an ID
             raise Exception(f"Failed to obtain session ID for client {client_id} after insert.")
    
    def end_session(self, session_id):
        """End a session"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE sessions SET end_time = CURRENT_TIMESTAMP WHERE id = ?",
                (session_id,)
            )
            conn.commit()
    
    def log_query(self, client_id, session_id, query_type, parameters=None):
        """Log a query"""
        parameters_str = json.dumps(parameters) if parameters else None
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO queries (client_id, session_id, query_type, parameters) VALUES (?, ?, ?, ?)",
                (client_id, session_id, query_type, parameters_str)
            )
            conn.commit()
            return cursor.lastrowid
    
    def add_message(self, sender_type, sender_id, recipient_type, recipient_id, message):
        """Add a message to the database"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO messages (sender_type, sender_id, recipient_type, recipient_id, message) VALUES (?, ?, ?, ?, ?)",
                (sender_type, sender_id, recipient_type, recipient_id, message)
            )
            conn.commit()
            return cursor.lastrowid
    
    def get_client_by_id(self, client_id):
        """Get client information by ID"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM clients WHERE id = ?", (client_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_all_clients(self):
        """Get all registered clients including their last login time."""
        try:
            # Use the context manager to ensure connection is returned
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                
                # Query to get all clients with their registration date, last login, and total queries
                query = """
                    SELECT 
                        c.id, 
                        c.nickname, 
                        c.registration_date as registration_date, -- Renamed for clarity
                        (SELECT MAX(s.start_time) FROM sessions s WHERE s.client_id = c.id) as last_seen, -- Get latest login time
                        (SELECT COUNT(*) FROM queries q WHERE q.client_id = c.id) as total_queries
                    FROM clients c
                    ORDER BY c.registration_date DESC;
                """
                
                cursor.execute(query)
                clients = cursor.fetchall()

                # Convert rows to dicts before connection context closes
                return [dict(row) for row in clients]
        except sqlite3.Error as sql_err:
             # Catch specific database errors
             logger.error(f"Database error fetching all clients: {sql_err}", exc_info=True)
             return []
        except Exception as e:
            # Catch any other unexpected errors (like TimeoutError from get_connection_context)
            logger.error(f"Error fetching all clients: {e}", exc_info=True)
            return []
    
    def get_client_queries(self, client_id):
        """Get all queries for a client"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM queries WHERE client_id = ? ORDER BY timestamp DESC",
                (client_id,)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_query_stats(self):
        """Get statistics about queries"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT query_type, COUNT(*) as count FROM queries GROUP BY query_type ORDER BY count DESC"
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_client_by_nickname(self, nickname):
        """Get client by nickname"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM clients WHERE nickname = ?", (nickname,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_active_sessions(self):
        """Get all active sessions"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT s.id as session_id, c.id as client_id, c.name, c.nickname, s.start_time as login_time, s.address as ip_address
            FROM sessions s
            JOIN clients c ON s.client_id = c.id
            WHERE s.end_time IS NULL
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_messages_for_client(self, client_id):
        """Get all unread messages for a client"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT id, sender_type, sender_id, message, timestamp
            FROM messages
            WHERE ((recipient_type = 'client' AND recipient_id = ?)
                OR recipient_type = 'all')
            AND read = 0
            ORDER BY timestamp DESC
            """, (client_id,))
            return [dict(row) for row in cursor.fetchall()]
    
    def mark_message_as_read(self, message_id):
        """Mark a message as read"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE messages SET read = 1 WHERE id = ?", (message_id,))
            conn.commit()
    
    def get_client_details_by_id(self, client_id):
        """Get detailed information for a specific client ID."""
        details = {}
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()

                # 1. Get basic client info
                cursor.execute("SELECT id, name, nickname, email, registration_date FROM clients WHERE id = ?", (client_id,))
                client_row = cursor.fetchone()
                if not client_row:
                    logger.warning(f"No client found with ID {client_id} for details.")
                    return None # Return None if client doesn't exist
                details = dict(zip([col[0] for col in cursor.description], client_row))

                cursor.execute("SELECT MAX(start_time) FROM sessions WHERE client_id = ?", (client_id,))
                last_login_result = cursor.fetchone()
                details['last_login'] = last_login_result[0] if last_login_result else None

                # 2. Get query statistics
                cursor.execute("""
                    SELECT query_type, COUNT(*) as count
                    FROM queries
                    WHERE client_id = ?
                    GROUP BY query_type
                    ORDER BY count DESC
                """, (client_id,))
                query_stats = cursor.fetchall()
                details['query_stats'] = [
                    dict(zip([col[0] for col in cursor.description], row))
                    for row in query_stats
                ]

                # 3. Get recent session history (e.g., last 10 sessions)
                cursor.execute("""
                    SELECT id, start_time, end_time, address,
                           (strftime('%s', end_time) - strftime('%s', start_time)) as duration_seconds
                    FROM sessions
                    WHERE client_id = ?
                    ORDER BY start_time DESC
                    LIMIT 10
                """, (client_id,))
                session_history = cursor.fetchall()
                details['session_history'] = [
                    dict(zip([col[0] for col in cursor.description], row))
                    for row in session_history
                ]

            logger.info(f"Successfully fetched details for client ID {client_id}")
            return details

        except sqlite3.Error as sql_err:
            logger.error(f"Database error fetching details for client ID {client_id}: {sql_err}", exc_info=True)
            # Return partial details if basic info was fetched?
            return details # Return whatever was fetched, might be just basic info or empty
        except Exception as e:
            logger.error(f"Unexpected error fetching details for client ID {client_id}: {e}", exc_info=True)
            return details # Return whatever was fetched

    def get_daily_query_counts(self):
        """Get daily counts for each query type."""
        query = """
            SELECT
                date(timestamp) as query_date,
                query_type,
                COUNT(*) as count
            FROM queries
            GROUP BY query_date, query_type
            ORDER BY query_date ASC, query_type ASC;
        """
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                return [dict(zip([col[0] for col in cursor.description], row)) for row in results]
        except sqlite3.Error as sql_err:
            logger.error(f"Database error fetching daily query counts: {sql_err}", exc_info=True)
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching daily query counts: {e}", exc_info=True)
            return [] 