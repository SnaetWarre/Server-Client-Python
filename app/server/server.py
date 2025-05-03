#!/usr/bin/env python3
# Server module for the arrest data client-server application

import os
import sys
import socket
import threading
import logging
import json
import time
import queue
from datetime import datetime
import sqlite3
import base64
import re # Import re module
import tempfile # <-- Add tempfile import
import uuid # Import uuid module

# Add the parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import shared modules
from shared.constants import *
from shared.protocol import Message, send_message, receive_message

# Import server modules
from .database import Database
from .data_processor import DataProcessor

# --- Configure logging to file ---
TEMP_DIR = tempfile.gettempdir()
SERVER_LOG_FILE = os.path.join(TEMP_DIR, 'server_temp.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=SERVER_LOG_FILE, # <-- Log to file
    filemode='w'              # <-- Overwrite file each time
    # handlers=[ ... ] # <-- Remove console handler
)
logger = logging.getLogger('server')

from .ClientHandler import ClientHandler

class Server:
    """Server for the arrest data client-server application"""
    
    def __init__(self, host=SERVER_HOST, port=SERVER_PORT):
        """Initialize server"""
        self.host = host
        self.port = port
        self.socket = None
        self.running = False
        self.clients = []  # List of active client handlers
        self.clients_lock = threading.Lock() # ADDED Lock for clients list
        self.db = Database()
        self.data_processor = DataProcessor() # Path to the processed data
        
        # Activity log for the server (for the GUI)
        self.activity_log = []
        self.activity_log_lock = threading.Lock()
        
        # Server GUI callbacks
        self.on_activity_log = None
        self.on_client_list_update = None
        self.on_all_clients_update = None # Callback for new registrations
    
    def start(self):
        """Start the server"""
        # If stop() was called earlier we will have closed the DB pool.
        # Reinitialize it here so logins/queries still work on restart.
        if getattr(self.db, '_closed', False):
            try:
                logger.info("Reinitializing database pool after previous stop()")
                # Re-create the Database instance (uses same db_path & pool_size)
                self.db = Database(self.db.db_path, self.db.pool_size)
            except Exception as e:
                logger.error(f"Failed to reinitialize database pool: {e}", exc_info=True)
                self.log_activity(f"Error reinitializing database: {e}")
                return False

        try:
            # Create socket
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind((self.host, self.port))
            self.socket.listen(5)
            
            self.running = True
            # Record server start time
            self.start_time = time.time()
            
            logger.info(f"Server started on {self.host}:{self.port}")
            self.log_activity(f"Server started on {self.host}:{self.port}")
            
            # Start accepting clients
            accept_thread = threading.Thread(target=self.accept_clients)
            accept_thread.daemon = True
            accept_thread.start()
            
            return True
        except Exception as e:
            logger.error(f"Error starting server: {e}")
            self.log_activity(f"Error starting server: {e}")
            return False
    
    def stop(self):
        """Stop the server"""
        logger.info("Server stopping...")
        self.running = False # Stop accepting new clients
        
        # Disconnect all clients
        logger.info(f"Disconnecting clients...")
        # Create a copy for safe iteration WHILE HOLDING THE LOCK
        clients_to_stop = []
        with self.clients_lock:
            clients_to_stop = list(self.clients)
            self.clients.clear() # Clear the main list immediately

        threads_to_join = []
        for client in clients_to_stop:
            threads_to_join.append(client) # Add thread to list for joining
            try:
                logger.debug(f"Signalling client handler {client.address} to stop.")
                client.running = False # Signal handler thread to stop
                if client.socket:
                     logger.debug(f"Shutting down and closing socket for {client.address}.")
                     # Shut down socket before closing to interrupt blocking calls
                     client.socket.shutdown(socket.SHUT_RDWR) 
                     client.socket.close()
            except (socket.error, OSError) as e:
                 # Log errors but continue trying to stop other clients
                 logger.error(f"Socket error stopping client {client.address}: {e}")
            except Exception as e:
                logger.error(f"Error signalling/closing client {client.address}: {e}", exc_info=True)

        # Wait for handler threads to finish
        logger.info(f"Waiting for {len(threads_to_join)} client handler thread(s) to join...")
        for thread in threads_to_join:
             try:
                  logger.debug(f"Joining thread for {thread.address}...")
                  thread.join(timeout=1.0) # Wait max 1 second per thread
                  if thread.is_alive():
                       logger.warning(f"Thread for {thread.address} did not join within timeout.")
                  else:
                       logger.debug(f"Thread for {thread.address} joined successfully.")
             except Exception as e:
                  logger.error(f"Error joining thread for {thread.address}: {e}", exc_info=True)
        logger.info("Finished joining client threads.")
        
        # Close server socket
        if self.socket:
            logger.info("Closing server socket...")
            try:
                self.socket.close()
                self.socket = None
                logger.info("Server socket closed.")
            except Exception as e:
                logger.error(f"Error closing server socket: {e}")
        
        # Close database connections in the pool
        if hasattr(self, 'db') and self.db:
            logger.info("Closing database connections...")
            try:
                self.db.close_all_connections()
            except Exception as e:
                logger.error(f"Error closing database connections: {e}", exc_info=True)
            
        logger.info("Server stopped")
        self.log_activity("Server stopped")
    
    def accept_clients(self):
        """Accept incoming client connections"""
        self.socket.settimeout(1.0)  # 1 second timeout for accepting clients
        
        while self.running:
            try:
                client_socket, client_address = self.socket.accept()
                
                # Set socket options for better reliability
                client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                
                # Log new connection attempt
                logger.info(f"Accepted connection from {client_address}")
                
                # Create and start a new client handler
                handler = ClientHandler(client_socket, client_address, self)
                handler.daemon = True
                handler.start()
            except socket.timeout:
                # This is expected due to the timeout
                pass
            except OSError as oe:
                if self.running:  # Only log if server is still supposed to be running
                    if oe.errno in (9, 22):  # Bad file descriptor or Invalid argument
                        logger.error(f"Socket error accepting client: {oe}")
                        # Socket might be closed, try to recreate
                        try:
                            if self.socket is None:
                                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                                self.socket.bind((self.host, self.port))
                                self.socket.listen(5)
                                self.socket.settimeout(1.0)
                                logger.info(f"Recreated server socket on {self.host}:{self.port}")
                        except Exception as e:
                            logger.error(f"Failed to recreate server socket: {e}")
                            time.sleep(5)  # Wait longer before retrying
                    else:
                        logger.error(f"OS error accepting client: {oe}")
                        time.sleep(1)  # Sleep a bit before retrying
            except Exception as e:
                if self.running:  # Only log if server is still supposed to be running
                    logger.error(f"Error accepting client: {e}", exc_info=True)
                
                # Sleep a bit to prevent high CPU usage on repeated errors
                time.sleep(0.5)
    
    def add_active_client(self, client_handler):
        """Add a client to the active clients list"""
        with self.clients_lock: # ACQUIRE LOCK
            if client_handler not in self.clients:
                self.clients.append(client_handler)
                logger.info(f"Added client {client_handler.address} to active list (now {len(self.clients)})")
            
        # Notify GUI if callback is set (outside lock if possible)
        if self.on_client_list_update:
            self.on_client_list_update()
    
    def remove_active_client(self, client_handler):
        """Remove a client from the active clients list"""
        removed = False
        with self.clients_lock: # ACQUIRE LOCK
            if client_handler in self.clients:
                self.clients.remove(client_handler)
                removed = True
                logger.info(f"Removed client {client_handler.address} from active list (now {len(self.clients)})")
        
        # Notify GUI if callback is set (outside lock if possible)
        if removed and self.on_client_list_update:
            self.on_client_list_update()
    
    def get_active_clients(self):
        """Get list of active clients including session start time"""
        active_clients = []
        with self.clients_lock:
            for client in list(self.clients):
                if client.client_info:
                    client_data = {
                        'id': client.client_info['id'],
                        'nickname': client.client_info['nickname'],
                        'name': client.client_info['name'],
                        'email': client.client_info['email'],
                        'session_id': client.session_id,
                        'address': f"{client.address[0]}:{client.address[1]}",
                        'connected_since': client.session_start_time
                    }
                    active_clients.append(client_data)
        return active_clients
    
    def broadcast_message(self, message_text):
        """Broadcast a message to all connected clients"""
        # Create message first
        message = Message(MSG_SERVER_MESSAGE, {
            'timestamp': datetime.now().isoformat(),
            'message': message_text
        })
        
        clients_to_send = []
        with self.clients_lock: # ACQUIRE LOCK
             # Get list of clients to send to under lock
             clients_to_send = [c for c in self.clients if c.client_info] # Create copy

        if not clients_to_send:
            logger.warning("No clients connected, message not sent")
            return False

        # Queue the message for each client (outside lock)
        for client in clients_to_send:
            client.queue_message(message)
            # Store the message in the database for each client (DB access is pooled/threadsafe)
            try:
                self.db.add_message(
                    sender_type='server',
                    sender_id=0,
                    recipient_type='client',
                    recipient_id=client.client_info['id'],
                    message=message_text
                )
            except Exception as db_err:
                 logger.error(f"DB Error adding broadcast message for client {client.client_info['id']}: {db_err}", exc_info=True)
        
        logger.info(f"Broadcast message queued for {len(clients_to_send)} clients")
        self.log_activity(f"Message broadcast to {len(clients_to_send)} clients: {message_text}")
        return True
    
    def send_message_to_client(self, client_id, message_text):
        """Send a message to a specific client"""
        target_client = None
        with self.clients_lock: # ACQUIRE LOCK
            # Find client under lock
            for client in self.clients:
                if client.client_info and client.client_info['id'] == client_id:
                    target_client = client
                    break
        
        if target_client:
            # Create and queue message outside lock
            message = Message(MSG_SERVER_MESSAGE, {
                'timestamp': datetime.now().isoformat(),
                'message': message_text
            })
            target_client.queue_message(message)
            
            # Store the message in the database (DB access is pooled/threadsafe)
            try:
                self.db.add_message(
                    sender_type='server',
                    sender_id=0,
                    recipient_type='client',
                    recipient_id=client_id,
                    message=message_text
                )
                logger.info(f"Message queued for client {target_client.client_info['nickname']}")
                self.log_activity(f"Message sent to client {target_client.client_info['nickname']}: {message_text}")
                return True
            except Exception as db_err:
                 logger.error(f"DB Error adding direct message for client {client_id}: {db_err}", exc_info=True)
                 # Indicate failure if DB log failed?
                 return False 
        else:
            logger.warning(f"Client with ID {client_id} not found or not logged in")
            return False
    
    def get_client_info(self, client_id):
        """Get client information from database"""
        return self.db.get_client_by_id(client_id)
    
    def get_all_clients(self):
        """Get all registered clients from database"""
        return self.db.get_all_clients()
    
    def get_client_details(self, client_id):
        """Get detailed information for a specific client by ID."""
        # Directly call the database method
        return self.db.get_client_details_by_id(client_id)
    
    def get_client_queries(self, client_id):
        """Get all queries for a client"""
        return self.db.get_client_queries(client_id)
    
    def get_query_stats(self):
        """Get statistics about queries"""
        stats = self.db.get_query_stats()
        
        # Add query descriptions
        for stat in stats:
            query_type = stat['query_type']
            if query_type in QUERY_DESCRIPTIONS:
                stat['description'] = QUERY_DESCRIPTIONS[query_type]
            else:
                stat['description'] = 'Unknown query type'
        
        return stats
    
    def get_daily_query_counts(self):
        """Get daily query counts per type from the database."""
        return self.db.get_daily_query_counts()
    
    def log_activity(self, message):
        """Log server activity"""
        timestamp = datetime.now().isoformat()
        
        with self.activity_log_lock:
            self.activity_log.append({
                'timestamp': timestamp,
                'message': message
            })
            
            # Keep only the last 100 log entries
            if len(self.activity_log) > 100:
                self.activity_log = self.activity_log[-100:]
        
        # Notify GUI if callback is set
        if self.on_activity_log:
            self.on_activity_log(timestamp, message)

    def notify_query_processed(self, client_id):
        """Called when a client query is processed, triggers an update of the 'All Clients' list."""
        logger.info(f"Query processed for client {client_id}, triggering all clients list update.")
        if self.on_all_clients_update:
            self.on_all_clients_update() # Reuse existing callback


if __name__ == "__main__":
    # Start the server directly if run as a script
    server = Server()
    server.start()
    
    try:
        # Keep the script running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping server...")
        server.stop() 
