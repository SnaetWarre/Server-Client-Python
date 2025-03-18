#!/usr/bin/env python3
# Database module for the server

import os
import sqlite3
import datetime
import pandas as pd
import threading
from pathlib import Path

class Database:
    """Thread-safe database for storing client information and query history"""
    
    def __init__(self, db_path='app/server/server_data.db'):
        """Initialize database connection and create tables if they don't exist"""
        self.db_path = db_path
        
        # Thread-local storage for database connections
        self.local = threading.local()
        
        # Lock for thread synchronization
        self.lock = threading.RLock()
        
        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Perform database migration if needed
        self.migrate_database()
        
        # Create tables using a temporary connection
        with self.get_connection() as conn:
            self.create_tables(conn)
    
    def migrate_database(self):
        """Perform necessary database migrations"""
        try:
            # Get a connection
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if sessions table exists and has the old schema
            cursor.execute("PRAGMA table_info(sessions)")
            columns = cursor.fetchall()
            
            # Get column names
            column_names = [col[1] for col in columns] if columns else []
            
            # If sessions table exists but doesn't have 'address' column
            if columns and 'address' not in column_names:
                # Drop the old sessions table as we're changing the structure
                # (another option would be to ALTER TABLE, but this is simpler for a new system)
                cursor.execute("DROP TABLE IF EXISTS sessions")
                conn.commit()
                print("Migrated database: dropped old sessions table")
            
            # Close the connection
            conn.close()
        except Exception as e:
            print(f"Error during database migration: {e}")
            # If migration fails, just continue with normal initialization
    
    def get_connection(self):
        """Get a database connection for the current thread"""
        if not hasattr(self.local, 'conn') or self.local.conn is None:
            self.local.conn = sqlite3.connect(self.db_path)
            # Enable foreign keys
            self.local.conn.execute("PRAGMA foreign_keys = ON")
            # Configure connection
            self.local.conn.row_factory = sqlite3.Row
        
        return self.local.conn
    
    def disconnect(self):
        """Disconnect the current thread's database connection"""
        if hasattr(self.local, 'conn') and self.local.conn is not None:
            self.local.conn.close()
            self.local.conn = None
    
    def create_tables(self, conn):
        """Create tables if they don't exist"""
        cursor = conn.cursor()
        
        # Create clients table
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
        
        # Create sessions table
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
        
        # Create queries table
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
        
        # Create messages table
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
        with self.lock:
            conn = self.get_connection()
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
            except Exception as e:
                conn.rollback()
                raise e
    
    def check_login(self, email, password):
        """Check login credentials and return client info if valid"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM clients WHERE email = ? AND password = ?", (email, password))
            row = cursor.fetchone()
            
            if row:
                return {
                    'id': row['id'],
                    'name': row['name'],
                    'nickname': row['nickname'],
                    'email': row['email'],
                    'registration_date': row['registration_date']
                }
            else:
                return None
    
    def start_session(self, client_id, address):
        """Start a new session for a client"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                "INSERT INTO sessions (client_id, address) VALUES (?, ?)",
                (client_id, address)
            )
            conn.commit()
            
            return cursor.lastrowid
    
    def end_session(self, session_id):
        """End a session"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                "UPDATE sessions SET end_time = CURRENT_TIMESTAMP WHERE id = ?",
                (session_id,)
            )
            conn.commit()
    
    def log_query(self, client_id, session_id, query_type, parameters=None):
        """Log a query"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Convert parameters to JSON string if provided
            if parameters:
                import json
                parameters_str = json.dumps(parameters)
            else:
                parameters_str = None
            
            cursor.execute(
                "INSERT INTO queries (client_id, session_id, query_type, parameters) VALUES (?, ?, ?, ?)",
                (client_id, session_id, query_type, parameters_str)
            )
            conn.commit()
            
            return cursor.lastrowid
    
    def add_message(self, sender_type, sender_id, recipient_type, recipient_id, message):
        """Add a message to the database"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                "INSERT INTO messages (sender_type, sender_id, recipient_type, recipient_id, message) VALUES (?, ?, ?, ?, ?)",
                (sender_type, sender_id, recipient_type, recipient_id, message)
            )
            conn.commit()
            
            return cursor.lastrowid
    
    def get_client_by_id(self, client_id):
        """Get client information by ID"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM clients WHERE id = ?", (client_id,))
            row = cursor.fetchone()
            
            if row:
                return {
                    'id': row['id'],
                    'name': row['name'],
                    'nickname': row['nickname'],
                    'email': row['email'],
                    'registration_date': row['registration_date']
                }
            else:
                return None
    
    def get_all_clients(self):
        """Get all registered clients"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM clients ORDER BY registration_date DESC")
            rows = cursor.fetchall()
            
            clients = []
            for row in rows:
                clients.append({
                    'id': row['id'],
                    'name': row['name'],
                    'nickname': row['nickname'],
                    'email': row['email'],
                    'registration_date': row['registration_date']
                })
            
            return clients
    
    def get_client_queries(self, client_id):
        """Get all queries for a client"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT * FROM queries WHERE client_id = ? ORDER BY timestamp DESC",
                (client_id,)
            )
            rows = cursor.fetchall()
            
            queries = []
            for row in rows:
                queries.append({
                    'query_id': row['id'],
                    'client_id': row['client_id'],
                    'session_id': row['session_id'],
                    'query_type': row['query_type'],
                    'parameters': row['parameters'],
                    'timestamp': row['timestamp']
                })
            
            return queries
    
    def get_query_stats(self):
        """Get statistics about queries"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT query_type, COUNT(*) as count FROM queries GROUP BY query_type ORDER BY count DESC"
            )
            rows = cursor.fetchall()
            
            stats = []
            for row in rows:
                stats.append({
                    'query_type': row['query_type'],
                    'count': row['count']
                })
            
            return stats
    
    def get_client_by_nickname(self, nickname):
        """Get client by nickname"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM clients WHERE nickname = ?", (nickname,))
            row = cursor.fetchone()
            
            if row:
                return {
                    'id': row['id'],
                    'name': row['name'],
                    'nickname': row['nickname'],
                    'email': row['email'],
                    'registration_date': row['registration_date']
                }
            else:
                return None
    
    def get_active_sessions(self):
        """Get all active sessions"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT s.id, c.id, c.name, c.nickname, s.start_time, s.address
            FROM sessions s
            JOIN clients c ON s.client_id = c.id
            WHERE s.end_time IS NULL
            """)
            rows = cursor.fetchall()
            
            sessions = []
            for row in rows:
                sessions.append({
                    'session_id': row['id'],
                    'client_id': row['id'],
                    'name': row['name'],
                    'nickname': row['nickname'],
                    'login_time': row['start_time'],
                    'ip_address': row['address']
                })
            
            return sessions
    
    def get_messages_for_client(self, client_id):
        """Get all unread messages for a client"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT id, sender_type, sender_id, message, timestamp
            FROM messages
            WHERE ((recipient_type = 'client' AND recipient_id = ?)
                OR recipient_type = 'all')
            AND read = 0
            ORDER BY timestamp DESC
            """, (client_id,))
            rows = cursor.fetchall()
            
            messages = []
            for row in rows:
                messages.append({
                    'message_id': row['id'],
                    'sender_type': row['sender_type'],
                    'sender_id': row['sender_id'],
                    'message': row['message'],
                    'timestamp': row['timestamp']
                })
            
            return messages
    
    def mark_message_as_read(self, message_id):
        """Mark a message as read"""
        with self.lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("UPDATE messages SET read = 1 WHERE id = ?", (message_id,))
            conn.commit() 