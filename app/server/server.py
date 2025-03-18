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

# Add the parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import shared modules
from shared.constants import *
from shared.protocol import Message, send_message, receive_message, encode_dataframe, encode_figure

# Import server modules
from .database import Database
from .data_processor import DataProcessor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('server')

class ClientHandler(threading.Thread):
    """Thread for handling a client connection"""
    
    def __init__(self, client_socket, client_address, server):
        super().__init__()
        self.socket = client_socket
        self.address = client_address
        self.server = server
        self.db = server.db  # This is now thread-safe
        self.data_processor = server.data_processor
        self.running = True
        self.client_info = None
        self.session_id = None
        
        # Client message queue (for messages from server to client)
        self.message_queue = queue.Queue()
        
        logger.info(f"New client connected from {client_address}")
    
    def run(self):
        """Main thread method"""
        try:
            last_queue_check = time.time()
            self.socket.settimeout(0.5)  # Increased timeout to reduce frequent timeouts
            
            # Log successful connection
            logger.info(f"Client handler started for {self.address}")
            
            while self.running:
                try:
                    # Receive message from client
                    message = receive_message(self.socket)
                    
                    if message is None:
                        # Client disconnected
                        logger.info(f"Client {self.address} disconnected")
                        self.running = False
                        break
                    
                    # Process message
                    self.process_message(message)
                except socket.timeout:
                    # No message received, just continue - this is expected behavior
                    pass
                except ConnectionError as ce:
                    # Connection error, client might have disconnected
                    logger.warning(f"Connection error from client {self.address}: {ce}")
                    self.running = False
                    break
                except Exception as e:
                    # Other unexpected errors
                    logger.error(f"Error receiving message from client {self.address}: {e}", exc_info=True)
                    # Don't break on every error - only if it's a connection error
                    if isinstance(e, (ConnectionError, ConnectionResetError, ConnectionAbortedError)):
                        self.running = False
                        break
                
                # Check if there are messages to send to the client
                # Do this regardless of whether we received a message
                current_time = time.time()
                if current_time - last_queue_check >= 0.1:  # Check every 100ms
                    self.check_message_queue()
                    last_queue_check = current_time
                
                # Sleep a bit to prevent high CPU usage
                time.sleep(0.01)
        except Exception as e:
            logger.error(f"Error in client handler main loop for {self.address}: {e}", exc_info=True)
        finally:
            # Close connection and clean up
            self.cleanup()
    
    def process_message(self, message):
        """Process a message from the client"""
        logger.info(f"Received message from {self.address}: {message.msg_type}")
        
        if message.msg_type == MSG_REGISTER:
            self.handle_register(message.data)
        elif message.msg_type == MSG_LOGIN:
            self.handle_login(message.data)
        elif message.msg_type == MSG_LOGOUT:
            self.handle_logout()
        elif message.msg_type == MSG_QUERY:
            self.handle_query(message.data)
        else:
            logger.warning(f"Unknown message type from {self.address}: {message.msg_type}")
    
    def handle_register(self, data):
        """Handle a registration request"""
        # Extract registration data
        name = data.get('name')
        nickname = data.get('nickname')
        email = data.get('email')
        password = data.get('password')
        
        # Validate data
        if not all([name, nickname, email, password]):
            self.send_error("Registration failed: missing required fields")
            return
        
        try:
            # Register client in database
            success = self.db.register_client(name, nickname, email, password)
            
            if success:
                logger.info(f"Registered new client: {nickname} ({email})")
                self.send_response(MSG_REGISTER, {
                    'status': STATUS_OK,
                    'message': "Registration successful"
                })
            else:
                logger.warning(f"Registration failed for {nickname} ({email}): Already exists")
                self.send_error("Registration failed: nickname or email already exists")
        except Exception as e:
            logger.error(f"Error during registration: {e}")
            self.send_error(f"Registration failed: {str(e)}")
    
    def handle_login(self, data):
        """Handle a login request"""
        # Extract login data
        email = data.get('email')
        password = data.get('password')
        
        # Validate data
        if not all([email, password]):
            self.send_error("Login failed: missing required fields")
            return
        
        try:
            # Check credentials
            client_info = self.db.check_login(email, password)
            
            if client_info:
                # Store client info
                self.client_info = client_info
                
                try:
                    # Start a new session
                    address_str = f"{self.address[0]}:{self.address[1]}"
                    self.session_id = self.db.start_session(
                        client_info['id'], 
                        address_str
                    )
                    
                    # Add to active clients
                    self.server.add_active_client(self)
                    
                    logger.info(f"Client logged in: {client_info['nickname']} ({client_info['email']})")
                    
                    # Send login response
                    self.send_response(MSG_LOGIN, {
                        'status': STATUS_OK,
                        'message': "Login successful",
                        'client_info': client_info
                    })
                    
                    # Log to server activity
                    self.server.log_activity(f"Client logged in: {client_info['nickname']} ({client_info['email']})")
                except sqlite3.OperationalError as sqlerr:
                    # Handle specific SQLite errors
                    error_msg = str(sqlerr)
                    logger.error(f"SQLite error during login: {error_msg}")
                    
                    if "no column named address" in error_msg:
                        self.send_error("Login failed: Database schema needs to be updated. Please restart the server.")
                    else:
                        self.send_error(f"Login failed: Database error - {error_msg}")
            else:
                logger.warning(f"Login failed for {email}")
                self.send_error("Login failed: invalid credentials")
        except Exception as e:
            logger.error(f"Error during login: {e}")
            self.send_error(f"Login failed: {str(e)}")
    
    def handle_logout(self):
        """Handle a logout request"""
        if self.client_info and self.session_id:
            try:
                # End session in database
                self.db.end_session(self.session_id)
                
                # Log the logout
                logger.info(f"Client logged out: {self.client_info['nickname']} ({self.client_info['email']})")
                
                # Remove from active clients
                self.server.remove_active_client(self)
                
                # Send logout response
                self.send_response(MSG_LOGOUT, {
                    'status': STATUS_OK,
                    'message': "Logout successful"
                })
                
                # Log to server activity
                self.server.log_activity(f"Client logged out: {self.client_info['nickname']} ({self.client_info['email']})")
                
                # Clear client info
                self.client_info = None
                self.session_id = None
            except Exception as e:
                logger.error(f"Error during logout: {e}")
                self.send_error(f"Logout failed: {str(e)}")
        else:
            self.send_error("Logout failed: not logged in")
    
    def handle_query(self, data):
        """Handle a query request"""
        if not self.client_info or not self.session_id:
            self.send_error("Query failed: not logged in")
            return
        
        # Extract query data
        query_type = data.get('query_type')
        parameters = data.get('parameters', {})
        
        if not query_type:
            self.send_error("Query failed: missing query type")
            return
        
        try:
            # Log the query
            query_id = self.db.log_query(
                self.client_info['id'],
                self.session_id,
                query_type,
                parameters
            )
            
            logger.info(f"Processing query from {self.client_info['nickname']}: {query_type}")
            
            # Process the query
            result = self.data_processor.process_query(query_type, parameters)
            
            if result.get('status') == 'error':
                logger.error(f"Query error: {result.get('message')}")
                self.send_error(f"Query failed: {result.get('message')}")
                return
            
            # Prepare response data
            response_data = {
                'status': STATUS_OK,
                'query_id': query_id,
                'query_type': query_type,
                'title': result.get('title', 'Query Results'),
                'message': "Query successful"
            }
            
            # Add dataframe if present
            if 'data' in result:
                response_data['data'] = encode_dataframe(result['data'])
            
            # Add figure if present
            if 'figure' in result:
                response_data['figure'] = encode_figure(result['figure'])
            
            # Add second figure if present
            if 'figure2' in result:
                response_data['figure2'] = encode_figure(result['figure2'])
            
            # Send response
            self.send_response(MSG_QUERY_RESULT, response_data)
            
            # Log to server activity
            self.server.log_activity(f"Query processed: {query_type} by {self.client_info['nickname']}")
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            self.send_error(f"Query failed: {str(e)}")
    
    def send_response(self, msg_type, data):
        """Send a response to the client"""
        message = Message(msg_type, data)
        send_message(self.socket, message)
    
    def send_error(self, error_message):
        """Send an error response to the client"""
        self.send_response(msg_type='ERROR', data={
            'status': STATUS_ERROR,
            'message': error_message
        })
    
    def queue_message(self, message):
        """Queue a message to be sent to the client"""
        self.message_queue.put(message)
    
    def check_message_queue(self):
        """Check and send queued messages"""
        try:
            messages_sent = 0
            max_messages_per_check = 10  # Limit the number of messages to process at once
            
            while not self.message_queue.empty() and messages_sent < max_messages_per_check:
                message = self.message_queue.get_nowait()
                
                try:
                    success = send_message(self.socket, message)
                    if success:
                        logger.info(f"Sent message to client {self.address}: {message.msg_type}")
                    else:
                        logger.error(f"Failed to send message to client {self.address}: {message.msg_type}")
                        # Put the message back in the queue for retry
                        self.message_queue.put(message)
                        # Break to avoid endlessly retrying the same failed message
                        break
                except socket.timeout:
                    logger.warning(f"Timeout sending message to client {self.address}: {message.msg_type}")
                    # Put the message back in the queue for retry
                    self.message_queue.put(message)
                    break
                except ConnectionError as ce:
                    logger.error(f"Connection error sending message to client {self.address}: {message.msg_type} - {ce}")
                    # Don't put message back - connection is likely broken
                    self.running = False
                    break
                except Exception as e:
                    logger.error(f"Error sending message to client {self.address}: {message.msg_type} - {e}")
                    # Only put the message back if it's not a connection error
                    if not isinstance(e, (ConnectionError, ConnectionResetError, ConnectionAbortedError)):
                        self.message_queue.put(message)
                    break
                
                self.message_queue.task_done()
                messages_sent += 1
                
            # If we processed the max messages, but there are still more in the queue,
            # log how many are left
            if messages_sent >= max_messages_per_check and not self.message_queue.empty():
                remaining = self.message_queue.qsize()
                if remaining > 0:
                    logger.info(f"Still {remaining} messages in queue for client {self.address}")
                    
        except Exception as e:
            logger.error(f"Error processing message queue for {self.address}: {e}", exc_info=True)
    
    def cleanup(self):
        """Clean up resources"""
        logger.info(f"Cleaning up connection for client {self.address}")
        
        # End session if client was logged in
        if self.client_info and self.session_id:
            try:
                self.db.end_session(self.session_id)
                logger.info(f"Session ended for {self.client_info['nickname']}")
                
                # Remove from active clients
                self.server.remove_active_client(self)
                
                # Log to server activity
                self.server.log_activity(f"Client disconnected: {self.client_info['nickname']} ({self.client_info['email']})")
            except Exception as e:
                logger.error(f"Error ending session: {e}")
        
        # Close socket
        try:
            if self.socket:
                self.socket.close()
                self.socket = None
        except Exception as e:
            logger.error(f"Error closing socket: {e}")
        
        # Make sure to release any database resources
        try:
            # If the handler has a database instance, ensure it disconnects the thread-local connection
            if hasattr(self, 'db') and self.db:
                self.db.disconnect()
        except Exception as e:
            logger.error(f"Error disconnecting database: {e}")


class Server:
    """Server for the arrest data client-server application"""
    
    def __init__(self, host=SERVER_HOST, port=SERVER_PORT):
        """Initialize server"""
        self.host = host
        self.port = port
        self.socket = None
        self.running = False
        self.clients = []  # List of active client handlers
        self.db = Database()
        self.data_processor = DataProcessor()
        
        # Activity log for the server (for the GUI)
        self.activity_log = []
        self.activity_log_lock = threading.Lock()
        
        # Server GUI callbacks
        self.on_activity_log = None
        self.on_client_list_update = None
    
    def start(self):
        """Start the server"""
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
        self.running = False
        
        # Disconnect all clients
        for client in self.clients[:]:  # Copy the list to avoid modification during iteration
            try:
                client.running = False
                if client.socket:
                    client.socket.close()
            except Exception as e:
                logger.error(f"Error disconnecting client: {e}")
        
        # Close server socket
        if self.socket:
            try:
                self.socket.close()
                self.socket = None
            except Exception as e:
                logger.error(f"Error closing server socket: {e}")
        
        # Clean up database connections
        try:
            # Database has its own thread-local connections, so we don't need to
            # explicitly close them here, but we can log that we're shutting down
            logger.info("Server database connections will be closed on thread exit")
        except Exception as e:
            logger.error(f"Error during database cleanup: {e}")
            
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
        if client_handler not in self.clients:
            self.clients.append(client_handler)
            
            # Notify GUI if callback is set
            if self.on_client_list_update:
                self.on_client_list_update()
    
    def remove_active_client(self, client_handler):
        """Remove a client from the active clients list"""
        if client_handler in self.clients:
            self.clients.remove(client_handler)
            
            # Notify GUI if callback is set
            if self.on_client_list_update:
                self.on_client_list_update()
    
    def get_active_clients(self):
        """Get list of active clients"""
        active_clients = []
        for client in self.clients:
            if client.client_info:
                active_clients.append({
                    'id': client.client_info['id'],
                    'nickname': client.client_info['nickname'],
                    'name': client.client_info['name'],
                    'email': client.client_info['email'],
                    'session_id': client.session_id,
                    'address': f"{client.address[0]}:{client.address[1]}"
                })
        return active_clients
    
    def broadcast_message(self, message_text):
        """Broadcast a message to all connected clients"""
        if not self.clients:
            logger.warning("No clients connected, message not sent")
            return False
        
        # Create a message
        message = Message(MSG_SERVER_MESSAGE, {
            'timestamp': datetime.now().isoformat(),
            'message': message_text
        })
        
        # Queue the message for each client
        for client in self.clients:
            if client.client_info:  # Only send to logged-in clients
                client.queue_message(message)
                
                # Store the message in the database for each client
                self.db.add_message(
                    sender_type='server',
                    sender_id=0,
                    recipient_type='client',
                    recipient_id=client.client_info['id'],
                    message=message_text
                )
        
        logger.info(f"Broadcast message to {len(self.clients)} clients")
        self.log_activity(f"Message broadcast to {len(self.clients)} clients: {message_text}")
        return True
    
    def send_message_to_client(self, client_id, message_text):
        """Send a message to a specific client"""
        for client in self.clients:
            if client.client_info and client.client_info['id'] == client_id:
                # Create a message
                message = Message(MSG_SERVER_MESSAGE, {
                    'timestamp': datetime.now().isoformat(),
                    'message': message_text
                })
                
                # Queue the message
                client.queue_message(message)
                
                # Store the message in the database
                self.db.add_message(
                    sender_type='server',
                    sender_id=0,
                    recipient_type='client',
                    recipient_id=client_id,
                    message=message_text
                )
                
                logger.info(f"Message sent to client {client.client_info['nickname']}")
                self.log_activity(f"Message sent to client {client.client_info['nickname']}: {message_text}")
                return True
        
        logger.warning(f"Client with ID {client_id} not found or not logged in")
        return False
    
    def get_client_info(self, client_id):
        """Get client information from database"""
        return self.db.get_client_by_id(client_id)
    
    def get_all_clients(self):
        """Get all registered clients from database"""
        return self.db.get_all_clients()
    
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