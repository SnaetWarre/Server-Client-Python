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
import struct
import json
import base64
import tempfile

# Add the parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import shared modules
from shared.constants import *
from shared.protocol import Message, send_message, receive_message

# Import database module

# --- Configure logging to file ---
TEMP_DIR = tempfile.gettempdir()
CLIENT_LOG_FILE = os.path.join(TEMP_DIR, 'client_temp.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=CLIENT_LOG_FILE, # <-- Log to file
    filemode='w'              # <-- Overwrite file each time
    # handlers=[ ... ] # <-- Remove console handler
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
            logger.error(f"Error connecting to server: {e}", exc_info=True)
            
            # Notify GUI if callback is set
            if self.on_error:
                self.on_error(f"Error connecting to server: {e}")
            
            # Clean up potentially partial connection
            logger.info("Connect failed, cleaning up potentially partial connection.")
            if self.socket:
                try:
                    self.socket.close()
                except OSError: pass
            self.socket = None
            self.connected = False
            self.running = False
            
            # Also notify GUI of failure again to ensure UI state is correct
            if self.on_connection_status_change:
                try:
                    self.on_connection_status_change(False)
                except Exception as cb_err:
                    logger.error(f"Error in connection status callback during cleanup: {cb_err}")
            
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
        if self.socket:
             # Temporarily disable default timeout for this check if needed,
             # though fileno() should work regardless.
             # original_timeout = self.socket.gettimeout()
             # self.socket.settimeout(None)
            pass # No need to change timeout just for fileno check

        consecutive_errors = 0
        max_consecutive_errors = 5

        while self.running:
            try:
                # --- ADD SOCKET CHECK LOGGING HERE ---
                socket_valid = False
                socket_fileno = -1 # Default invalid
                if self.socket:
                    try:
                        socket_fileno = self.socket.fileno() # Get OS file descriptor number
                        if socket_fileno != -1:
                             socket_valid = True
                    except Exception as sock_err:
                        logger.warning(f"RECEIVER: Error checking socket status before receive: {sock_err}")

                logger.info(f"RECEIVER LOOP: Socket valid check: {socket_valid}, Fileno: {socket_fileno}. Attempting receive...")
                if not socket_valid:
                     logger.error("RECEIVER LOOP: Socket is invalid or None before calling receive_message. Breaking loop.")
                     self.running = False # Ensure loop stops
                     break # Exit loop immediately if socket is bad
                # --- END SOCKET CHECK LOGGING ---

                # Receive message - This is where the error occurs
                message = receive_message(self.socket)

                if message is None:
                    logger.warning("Connection closed by server (receive_message returned None)")
                    break # Exit loop if connection closed gracefully

                # Process message
                self.process_message(message)
                consecutive_errors = 0
                time.sleep(0.01)

            except socket.timeout:
                pass # Expected, just continue loop

            except (ConnectionError, ValueError, OSError, struct.error, json.JSONDecodeError) as e:
                 # Catch OSError here explicitly
                 logger.error(f"Error receiving messages: {e}", exc_info=True)
                 consecutive_errors += 1
                 if consecutive_errors >= max_consecutive_errors or isinstance(e, OSError) and e.errno == 9: # Break immediately on Errno 9
                     logger.error(f"Too many errors or fatal socket error (Errno 9), disconnecting receiver loop.")
                     break # Exit the loop on fatal errors
                 time.sleep(0.5)
            # No general Exception catch here to avoid hiding problems

        # Connection lost or loop exited
        if self.connected:
             logger.warning("Receiver loop finished or connection lost, disconnecting client.")
             # Use a separate method or flag to trigger disconnect from the main thread if needed,
             # or ensure disconnect logic here is robust.
             # Avoid calling self.disconnect() directly from this thread if it modifies GUI elements.
             # For now, just update status flags.
             self.connected = False
             self.logged_in = False
             self.running = False # Ensure running is false

             # Use the bridge to signal connection loss to the GUI thread safely
             if self.on_connection_status_change:
                  self.on_connection_status_change(False)
             if self.on_error:
                  self.on_error("Connection lost or socket error")

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
                # Decode data/plot *before* calling the GUI callback
                processed_data = message.data.copy() # Start with a copy
                encoded_df_data = processed_data.get('data')
                encoded_plot_data = processed_data.get('plot')

                if isinstance(encoded_df_data, str):
                    try:
                        processed_data['data'] = decode_dataframe(encoded_df_data)
                        logger.debug("PROCESS_MESSAGE: Decoded dataframe successfully.")
                    except Exception as e:
                        logger.error(f"PROCESS_MESSAGE: Error decoding dataframe: {e}", exc_info=True)
                        processed_data['data'] = [] # Replace with empty list on error
                        processed_data['error'] = processed_data.get('error', '') + f" Client error: Failed to decode results ({e})"
                
                if isinstance(encoded_plot_data, str):
                    try:
                        # Decode base64 string to bytes for display
                        processed_data['plot'] = base64.b64decode(encoded_plot_data.encode('utf-8'))
                        logger.debug("PROCESS_MESSAGE: Decoded plot data successfully.")
                    except Exception as e:
                        logger.error(f"PROCESS_MESSAGE: Error decoding plot data: {e}", exc_info=True)
                        processed_data['plot'] = None # Set plot to None on error
                        processed_data['error'] = processed_data.get('error', '') + f" Client error: Failed to decode plot ({e})"

                # Now call the callback with the *processed* data
                if self.on_query_result:
                    logger.info(f"PROCESS_MESSAGE (Query Result): Calling on_query_result callback. Processed keys: {list(processed_data.keys())}")
                    self.on_query_result(processed_data)
                
                # Log success using original (or processed) data for context
                query_type = processed_data.get('query_type', 'unknown')
                metadata_type = processed_data.get('metadata_type', 'N/A')
                log_identifier = metadata_type if metadata_type != 'N/A' else query_type
                logger.info(f"Received successful query/metadata result for {log_identifier}")
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
    
    def send_request(self, request_data):
        """Send a generic request dictionary to the server."""
        if not self.connected:
            logger.error("Cannot send request: Not connected to server")
            # Optionally notify GUI via on_error callback
            if self.on_error:
                self.on_error("Cannot send request: Not connected")
            return False

        command = request_data.get('command')
        if not command:
            logger.error("Cannot send request: 'command' key missing in request data")
            if self.on_error:
                self.on_error("Internal error: Command missing in request")
            return False

        # Determine message type based on command
        msg_type = None
        if command == 'query':
            msg_type = MSG_QUERY
            # Query requires login
            if not self.logged_in:
                 logger.error("Cannot send query: Not logged in")
                 if self.on_error:
                      self.on_error("Cannot send query: Not logged in")
                 return False
        elif command == 'get_metadata':
            msg_type = MSG_GET_METADATA # Use the new constant
            # Metadata might also require login, adjust if needed
            if not self.logged_in:
                 logger.error("Cannot get metadata: Not logged in")
                 if self.on_error:
                      self.on_error("Cannot get metadata: Not logged in")
                 return False
        # Add other command mappings here if needed (e.g., login, register)
        # else if command == 'login': msg_type = MSG_LOGIN
        # else if command == 'register': msg_type = MSG_REGISTER
        else:
            logger.error(f"Cannot send request: Unknown command '{command}'")
            if self.on_error:
                 self.on_error(f"Internal error: Unknown command '{command}'")
            return False

        # Prepare the data payload (remove the 'command' key)
        payload = request_data.copy()
        del payload['command']

        # Create the message
        message = Message(msg_type, payload)

        # Send the message using the existing low-level function
        logger.info(f"Sending request: Type={msg_type}, Payload={payload}")
        try:
            success = send_message(self.socket, message)
            if not success:
                logger.error(f"Failed to send request (command: {command}) using send_message.")
                # Trigger error callback if send fails
                if self.on_error:
                     self.on_error(f"Failed to send request for command '{command}'")
                return False
            return True
        except Exception as e:
             logger.error(f"Exception sending request (command: {command}): {e}", exc_info=True)
             if self.on_error:
                  self.on_error(f"Network error sending request: {e}")
             # Handle potential disconnect on send error
             self.connected = False
             self.running = False
             if self.on_connection_status_change:
                  self.on_connection_status_change(False)
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
                        
                        client.send_request({'command': 'query', 'query_type': query_type, 'parameters': params})
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