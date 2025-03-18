#!/usr/bin/env python3
# Client module for the arrest data client-server application

import os
import sys
import socket
import threading
import queue
import time
import logging
from datetime import datetime

# Add the parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import shared modules
from shared.constants import *
from shared.protocol import Message, send_message, receive_message, decode_dataframe, decode_figure

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('client')


class Client:
    """Client for the arrest data client-server application"""
    
    def __init__(self, host=SERVER_HOST, port=SERVER_PORT):
        """Initialize client"""
        self.host = host
        self.port = port
        self.socket = None
        self.running = False
        self.connected = False
        self.logged_in = False
        self.client_info = None
        
        # Queue for incoming messages
        self.message_queue = queue.Queue()
        
        # Callbacks for the GUI
        self.on_connection_status_change = None
        self.on_login_status_change = None
        self.on_message_received = None
        self.on_query_result = None
        self.on_error = None
        
        # Receiver thread
        self.receiver_thread = None
    
    def connect(self):
        """Connect to the server"""
        try:
            # Create socket
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            
            # Set flags
            self.connected = True
            self.running = True
            
            # Notify GUI if callback is set
            if self.on_connection_status_change:
                self.on_connection_status_change(True)
            
            # Start receiver thread
            self.receiver_thread = threading.Thread(target=self.receive_messages)
            self.receiver_thread.daemon = True
            self.receiver_thread.start()
            
            logger.info(f"Connected to server at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to server: {e}")
            
            # Notify GUI if callback is set
            if self.on_error:
                self.on_error(f"Error connecting to server: {e}")
            
            return False
    
    def disconnect(self):
        """Disconnect from the server"""
        # Logout first if logged in
        if self.logged_in:
            self.logout()
        
        # Stop receiver thread
        self.running = False
        
        # Close socket
        if self.socket:
            try:
                self.socket.close()
            except Exception as e:
                logger.error(f"Error closing socket: {e}")
        
        # Reset flags
        self.connected = False
        self.logged_in = False
        self.client_info = None
        
        # Notify GUI if callback is set
        if self.on_connection_status_change:
            self.on_connection_status_change(False)
        
        logger.info("Disconnected from server")
    
    def receive_messages(self):
        """Receive messages from the server in a separate thread"""
        # Set socket timeout to prevent blocking forever
        if self.socket:
            self.socket.settimeout(0.5)  # 0.5 second timeout
            
        consecutive_errors = 0
        max_consecutive_errors = 5  # Allow up to 5 consecutive errors before disconnecting
        
        while self.running:
            try:
                if not self.socket:
                    logger.warning("Socket is None, stopping receiver thread")
                    break
                
                # Receive message
                message = receive_message(self.socket)
                
                if message is None:
                    # Connection closed by server
                    logger.warning("Connection closed by server")
                    break
                
                # Process message
                self.process_message(message)
                
                # Reset error counter on successful message processing
                consecutive_errors = 0
                
                # Sleep a bit to prevent high CPU usage
                time.sleep(0.01)
            except socket.timeout:
                # Timeout is expected, just continue
                pass
            except ConnectionError as ce:
                logger.error(f"Connection error: {ce}")
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"Too many consecutive connection errors, disconnecting")
                    break
            except Exception as e:
                logger.error(f"Error receiving messages: {e}", exc_info=True)
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"Too many consecutive errors, disconnecting")
                    break
                time.sleep(0.5)  # Sleep longer on error
        
        # Connection lost
        if self.connected:
            logger.warning("Connection lost, disconnecting")
            self.connected = False
            self.logged_in = False
            self.client_info = None
            
            # Notify GUI if callbacks are set
            if self.on_connection_status_change:
                self.on_connection_status_change(False)
            
            if self.on_login_status_change:
                self.on_login_status_change(False)
            
            if self.on_error:
                self.on_error("Connection lost")
                
            # Add message to queue
            self.message_queue.put({
                'type': 'error',
                'timestamp': datetime.now().isoformat(),
                'message': "Connection to server lost"
            })
    
    def process_message(self, message):
        """Process a message from the server"""
        logger.info(f"Received message from server: {message.msg_type}")
        
        try:
            if message.msg_type == MSG_LOGIN:
                self.handle_login_response(message.data)
            elif message.msg_type == MSG_LOGOUT:
                self.handle_logout_response(message.data)
            elif message.msg_type == MSG_REGISTER:
                self.handle_register_response(message.data)
            elif message.msg_type == MSG_QUERY_RESULT:
                self.handle_query_result(message.data)
            elif message.msg_type == MSG_SERVER_MESSAGE:
                self.handle_server_message(message.data)
            elif message.msg_type == 'ERROR':
                self.handle_error(message.data)
            else:
                logger.warning(f"Unknown message type: {message.msg_type}")
                # Add to message queue for unknown messages
                self.message_queue.put({
                    'type': 'info',
                    'timestamp': datetime.now().isoformat(),
                    'message': f"Received unknown message type: {message.msg_type}"
                })
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            # Add error to message queue
            self.message_queue.put({
                'type': 'error',
                'timestamp': datetime.now().isoformat(),
                'message': f"Error processing message: {e}"
            })
    
    def handle_login_response(self, data):
        """Handle login response from the server"""
        status = data.get('status')
        
        if status == STATUS_OK:
            self.logged_in = True
            self.client_info = data.get('client_info')
            
            # Notify GUI if callback is set
            if self.on_login_status_change:
                self.on_login_status_change(True)
            
            logger.info(f"Logged in as {self.client_info['nickname']}")
            
            # Add to message queue
            self.message_queue.put({
                'type': 'info',
                'timestamp': datetime.now().isoformat(),
                'message': f"Logged in as {self.client_info['nickname']}"
            })
        else:
            error_message = data.get('message', 'Login failed')
            
            # Notify GUI if callback is set
            if self.on_error:
                self.on_error(error_message)
            
            logger.error(f"Login failed: {error_message}")
            
            # Add to message queue
            self.message_queue.put({
                'type': 'error',
                'timestamp': datetime.now().isoformat(),
                'message': f"Login failed: {error_message}"
            })
    
    def handle_logout_response(self, data):
        """Handle logout response from the server"""
        status = data.get('status')
        
        if status == STATUS_OK:
            self.logged_in = False
            self.client_info = None
            
            # Notify GUI if callback is set
            if self.on_login_status_change:
                self.on_login_status_change(False)
            
            logger.info("Logged out")
            
            # Add to message queue
            self.message_queue.put({
                'type': 'info',
                'timestamp': datetime.now().isoformat(),
                'message': "Logged out"
            })
        else:
            error_message = data.get('message', 'Logout failed')
            
            # Notify GUI if callback is set
            if self.on_error:
                self.on_error(error_message)
            
            logger.error(f"Logout failed: {error_message}")
            
            # Add to message queue
            self.message_queue.put({
                'type': 'error',
                'timestamp': datetime.now().isoformat(),
                'message': f"Logout failed: {error_message}"
            })
    
    def handle_register_response(self, data):
        """Handle register response from the server"""
        status = data.get('status')
        
        if status == STATUS_OK:
            logger.info("Registration successful")
            
            # Add to message queue
            self.message_queue.put({
                'type': 'info',
                'timestamp': datetime.now().isoformat(),
                'message': "Registration successful. You can now log in."
            })
        else:
            error_message = data.get('message', 'Registration failed')
            
            # Notify GUI if callback is set
            if self.on_error:
                self.on_error(error_message)
            
            logger.error(f"Registration failed: {error_message}")
            
            # Add to message queue
            self.message_queue.put({
                'type': 'error',
                'timestamp': datetime.now().isoformat(),
                'message': f"Registration failed: {error_message}"
            })
    
    def handle_query_result(self, data):
        """Handle query result from the server"""
        status = data.get('status')
        
        if status == STATUS_OK:
            query_type = data.get('query_type')
            query_id = data.get('query_id')
            title = data.get('title', 'Query Results')
            
            # Process data and figures if present
            result = {
                'query_type': query_type,
                'query_id': query_id,
                'title': title
            }
            
            # Decode dataframe if present
            if 'data' in data:
                try:
                    result['data'] = decode_dataframe(data['data'])
                except Exception as e:
                    logger.error(f"Error decoding dataframe: {e}")
                    result['data'] = None
            
            # Decode figure if present
            if 'figure' in data:
                try:
                    result['figure'] = decode_figure(data['figure'])
                except Exception as e:
                    logger.error(f"Error decoding figure: {e}")
                    result['figure'] = None
            
            # Decode second figure if present
            if 'figure2' in data:
                try:
                    result['figure2'] = decode_figure(data['figure2'])
                except Exception as e:
                    logger.error(f"Error decoding figure2: {e}")
                    result['figure2'] = None
            
            # Notify GUI if callback is set
            if self.on_query_result:
                self.on_query_result(result)
            
            logger.info(f"Received query result for {query_type}")
            
            # Add to message queue
            self.message_queue.put({
                'type': 'info',
                'timestamp': datetime.now().isoformat(),
                'message': f"Received query result for {query_type}"
            })
        else:
            error_message = data.get('message', 'Query failed')
            
            # Notify GUI if callback is set
            if self.on_error:
                self.on_error(error_message)
            
            logger.error(f"Query failed: {error_message}")
            
            # Add to message queue
            self.message_queue.put({
                'type': 'error',
                'timestamp': datetime.now().isoformat(),
                'message': f"Query failed: {error_message}"
            })
    
    def handle_server_message(self, data):
        """Handle server message"""
        message_text = data.get('message', '')
        timestamp = data.get('timestamp', datetime.now().isoformat())
        
        logger.info(f"Received server message: {message_text}")
        
        # We have two approaches to handle messages:
        # 1. Through the direct callback (this is used for immediate display)
        # 2. Through the message queue (this is used for queued/delayed processing)
        # To avoid duplication, we'll only use one approach based on whether a callback is set
        
        # If we have a callback, use it directly and don't add to the queue
        callback_success = False
        if self.on_message_received:
            try:
                self.on_message_received(timestamp, message_text)
                logger.info("Successfully called on_message_received callback")
                callback_success = True
            except Exception as e:
                logger.error(f"Error in on_message_received callback: {e}")
                # Callback failed, so we'll use the queue instead
        
        # If callback failed or there is no callback, use the message queue
        if not callback_success:
            logger.info("Using message queue for server message")
            self.message_queue.put({
                'type': 'server',
                'timestamp': timestamp,
                'message': message_text,
                'use_queue': True  # Flag to indicate this should be processed from the queue
            })
    
    def handle_error(self, data):
        """Handle error message from the server"""
        error_message = data.get('message', 'Unknown error')
        
        # Notify GUI if callback is set
        if self.on_error:
            self.on_error(error_message)
        
        logger.error(f"Server error: {error_message}")
        
        # Add to message queue
        self.message_queue.put({
            'type': 'error',
            'timestamp': datetime.now().isoformat(),
            'message': f"Server error: {error_message}"
        })
    
    def register(self, name, nickname, email, password):
        """Register a new user"""
        if not self.connected:
            logger.error("Not connected to server")
            return False
        
        # Create message
        message = Message(MSG_REGISTER, {
            'name': name,
            'nickname': nickname,
            'email': email,
            'password': password
        })
        
        # Send message
        if send_message(self.socket, message):
            logger.info(f"Sent registration request for {nickname} ({email})")
            return True
        else:
            logger.error(f"Failed to send registration request")
            return False
    
    def login(self, email, password):
        """Log in to the server"""
        if not self.connected:
            logger.error("Not connected to server")
            return False
        
        if self.logged_in:
            logger.warning("Already logged in")
            return False
        
        # Create message
        message = Message(MSG_LOGIN, {
            'email': email,
            'password': password
        })
        
        # Send message
        if send_message(self.socket, message):
            logger.info(f"Sent login request for {email}")
            return True
        else:
            logger.error(f"Failed to send login request")
            return False
    
    def logout(self):
        """Log out from the server"""
        if not self.connected:
            logger.error("Not connected to server")
            return False
        
        if not self.logged_in:
            logger.warning("Not logged in")
            return False
        
        # Create message
        message = Message(MSG_LOGOUT, {})
        
        # Send message
        if send_message(self.socket, message):
            logger.info("Sent logout request")
            return True
        else:
            logger.error("Failed to send logout request")
            return False
    
    def send_query(self, query_type, parameters=None):
        """Send a query to the server"""
        if not self.connected:
            logger.error("Not connected to server")
            return False
        
        if not self.logged_in:
            logger.error("Not logged in")
            return False
        
        # Create message
        message = Message(MSG_QUERY, {
            'query_type': query_type,
            'parameters': parameters if parameters else {}
        })
        
        # Send message
        if send_message(self.socket, message):
            logger.info(f"Sent query: {query_type}")
            return True
        else:
            logger.error(f"Failed to send query: {query_type}")
            return False
    
    def get_next_message(self):
        """Get the next message from the queue"""
        try:
            return self.message_queue.get_nowait()
        except queue.Empty:
            return None


if __name__ == "__main__":
    # Simple test client
    client = Client()
    
    try:
        # Connect to server
        if client.connect():
            print("Connected to server")
            
            # Wait for input
            while True:
                cmd = input("Enter command (r=register, l=login, o=logout, q=query, x=exit): ")
                
                if cmd == 'r':
                    # Register
                    name = input("Name: ")
                    nickname = input("Nickname: ")
                    email = input("Email: ")
                    password = input("Password: ")
                    
                    client.register(name, nickname, email, password)
                elif cmd == 'l':
                    # Login
                    email = input("Email: ")
                    password = input("Password: ")
                    
                    client.login(email, password)
                elif cmd == 'o':
                    # Logout
                    client.logout()
                elif cmd == 'q':
                    # Query
                    if client.logged_in:
                        query_type = input("Query type: ")
                        
                        # Check if parameters are needed
                        params = {}
                        if query_type in ['top_charge_groups', 'arrests_by_area']:
                            n = input("Number of results (default: 10): ")
                            if n:
                                params['n'] = int(n)
                        elif query_type == 'arrests_by_month':
                            year = input("Year (leave empty for all years): ")
                            if year:
                                params['year'] = int(year)
                        
                        client.send_query(query_type, params)
                    else:
                        print("You must be logged in to send queries")
                elif cmd == 'x':
                    # Exit
                    break
                
                # Check for messages
                while True:
                    message = client.get_next_message()
                    if message:
                        print(f"[{message['type']}] {message['timestamp']}: {message['message']}")
                    else:
                        break
                
                # Sleep a bit to prevent high CPU usage
                time.sleep(0.1)
        else:
            print("Failed to connect to server")
    finally:
        # Disconnect from server
        client.disconnect()
        print("Disconnected from server") 