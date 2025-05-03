import threading
import socket
import queue
import time
import sqlite3
import re
import logging
import os
import tempfile
import sys

from datetime import datetime
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



class ClientHandler(threading.Thread):
    """Thread for handling a client connection"""
    
    def __init__(self, client_socket, client_address, server):
        super().__init__()
        self.socket = client_socket
        self.address = client_address
        self.server = server
        self.db = server.db
        self.data_processor = server.data_processor
        self.running = True
        self.client_info = None
        self.session_id = None
        self.session_start_time = None
        self.connection_lost = False
        self.was_logged_in = False
        
        # Client message queue (for messages from server to client)
        self.message_queue = queue.Queue()
        
        logger.info(f"New client connected from {client_address}")
    
    def run(self):
        """Main thread method"""
        logger.info(f"HANDLER: Thread started for client {self.address}")
        client_removed_from_list = False
        try:
            # --- SEND WELCOME/CONFIRMATION MESSAGE ---
            try:
                # Example: Send a simple confirmation or server info
                # You could define a new message type like MSG_CONNECTED_OK
                # For simplicity, we reuse SERVER_MESSAGE for now
                connect_ok_msg = Message(MSG_SERVER_MESSAGE, {
                    'timestamp': datetime.now().isoformat(),
                    'message': "Connection accepted."
                })
                send_message(self.socket, connect_ok_msg)
                logger.info(f"HANDLER: Sent connection confirmation to {self.address}")
            except Exception as send_err:
                 logger.error(f"HANDLER: Failed to send initial confirmation to {self.address}: {send_err}", exc_info=True)
                 self.running = False # Cannot proceed if initial send failed
            # ------------------------------------------

            last_queue_check = time.time()
            self.socket.settimeout(0.1)

            logger.info(f"HANDLER: Entering main loop for client {self.address}")

            while self.running:
                message = None
                try:
                    message = receive_message(self.socket)

                    if message is None:
                        logger.info(f"HANDLER: Client {self.address} disconnected (receive_message returned None)")
                        self.running = False
                        self.connection_lost = True
                        break

                    self.process_message(message)

                except socket.timeout:
                    pass

                except ConnectionAbortedError:
                    logger.warning(f"HANDLER: Connection aborted by client {self.address}.")
                    self.running = False
                    self.connection_lost = True
                    break
                except ConnectionResetError:
                    logger.warning(f"HANDLER: Connection reset by client {self.address}.")
                    self.running = False
                    self.connection_lost = True
                    break
                except BrokenPipeError:
                    logger.warning(f"HANDLER: Broken pipe for client {self.address} (likely disconnected).")
                    self.running = False
                    self.connection_lost = True
                    break
                except (socket.error, OSError) as sock_err: # Catch specific socket/OS errors
                    logger.error(f"HANDLER: Socket/OS Error for client {self.address}: {sock_err}. Terminating handler.")
                    self.running = False # Ensure loop termination on socket errors
                    self.connection_lost = True
                    break
                except Exception as e:
                    # Log other unexpected errors but allow loop to potentially continue
                    # if it wasn't a direct socket/connection issue.
                    logger.error(f"HANDLER: Non-socket error processing/receiving message from client {self.address}: {e}", exc_info=True)
                    time.sleep(0.1) # Brief pause

                current_time = time.time()
                if current_time - last_queue_check >= 0.1:
                    try:
                        self.check_message_queue()
                    except Exception as q_err:
                        logger.error(f"HANDLER: Error checking/sending message queue for {self.address}: {q_err}", exc_info=True)
                    last_queue_check = current_time

            # --- Remove client from list BEFORE finally block --- 
            if self.was_logged_in:
                 try:
                      logger.debug(f"HANDLER: Removing client {self.address} from server list before cleanup.")
                      self.server.remove_active_client(self)
                      client_removed_from_list = True
                 except Exception as remove_err:
                      logger.error(f"HANDLER: Error removing client {self.address} from list before cleanup: {remove_err}", exc_info=True)
            # ---------------------------------------------------

            logger.info(f"HANDLER: Exited main loop for client {self.address}. Running state: {self.running}")

        except Exception as e:
            logger.error(f"HANDLER: Unhandled exception in handler main try block for {self.address}: {e}", exc_info=True)
            # Ensure removal if unexpected error occurred while logged in
            if self.was_logged_in and not client_removed_from_list:
                 try:
                      logger.debug(f"HANDLER: Removing client {self.address} from server list after main exception.")
                      self.server.remove_active_client(self)
                 except Exception as remove_err:
                      logger.error(f"HANDLER: Error removing client {self.address} from list after main exception: {remove_err}", exc_info=True)
        finally:
            logger.info(f"HANDLER: Entering finally block for cleanup of client {self.address}. Logged in: {self.client_info is not None} / Flag: {self.was_logged_in}")
            self.cleanup(client_removed_before_cleanup=client_removed_from_list)
    
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
        elif message.msg_type == MSG_GET_METADATA:
            self.handle_get_metadata(message.data)
        else:
            logger.warning(f"Unknown message type from {self.address}: {message.msg_type}")
    
    def handle_register(self, data):
        """Handle a registration request"""
        # Extract registration data
        name = data.get('name')
        nickname = data.get('nickname')
        email = data.get('email')
        password = data.get('password')
        
        # Get the request ID sent by the client
        request_id = data.get('request_id')
        
        # Validate data
        if not all([name, nickname, email, password]):
            self.send_error("Registration failed: missing required fields", request_id=request_id)
            return
        
        # Basic email format validation using regex
        email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$" # Corrected closing quote
        if not re.match(email_regex, email):
             self.send_error("Registration failed: invalid email format", request_id=request_id)
             return

        try:
            # Register client in database
            success = self.db.register_client(name, nickname, email, password)
            
            if success:
                logger.info(f"Registered new client: {nickname} ({email})")
                self.send_response(MSG_REGISTER, {
                    'status': STATUS_OK,
                    'message': "Registration successful",
                    'request_id': request_id # Include request ID in response
                })
                # Trigger the callback to notify GUI about new client registration
                if self.server.on_all_clients_update:
                    self.server.on_all_clients_update()
            else:
                logger.warning(f"Registration failed for {nickname} ({email}): Already exists")
                self.send_error("Registration failed: nickname or email already exists", request_id=request_id)
        except Exception as e:
            logger.error(f"Error during registration: {e}")
            self.send_error(f"Registration failed: {str(e)}", request_id=request_id)
    
    def handle_login(self, data):
        """Handle a login request"""
        # Prevent duplicate login processing (patch added)
        if getattr(self, 'client_info', None) is not None:
            logger.warning(f"Duplicate login attempt ignored for client {self.address}")
            return
            
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
                    session_info = self.db.start_session(
                        client_info['id'], 
                        address_str
                    )
                    self.session_id = session_info.get('id')
                    self.session_start_time = session_info.get('start_time')
                    if not self.session_id:
                        raise Exception("Failed to retrieve session ID after starting session.")
                    
                    # Add to active clients
                    self.server.add_active_client(self)
                    self.was_logged_in = True
                    
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
                self.session_start_time = None
                self.was_logged_in = False
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

        # Query type is now directly in the data dictionary from the GUI
        query_type_id = data.get('query_type') # e.g., 'query1', 'query2'
        # The actual parameters are also directly in the data dictionary
        parameters = data # Pass the whole dictionary as parameters

        if not query_type_id:
            self.send_error("Query failed: missing query type identifier")
            return

        try:
            # Log the query using the new identifier
            # Note: parameters is the full dict from client, store as JSON string
            query_id = self.db.log_query(
                self.client_info['id'],
                self.session_id,
                query_type_id, # Log 'query1', 'query2', etc.
                parameters # Store the raw parameters received
            )

            logger.info(f"Processing query {query_type_id} from {self.client_info['nickname']} with params: {parameters}")

            # --- Process the query based on query_type_id ---
            result = {}
            if query_type_id == 'query1':
                 result = self.data_processor.process_query1(parameters)
            elif query_type_id == 'query2':
                 result = self.data_processor.process_query2(parameters)
            elif query_type_id == 'query3':
                 result = self.data_processor.process_query3(parameters) # This one needs plot generation
            elif query_type_id == 'query4':
                 result = self.data_processor.process_query4(parameters)
            else:
                 # Handle unknown queryX type
                 logger.error(f"Unknown query type identifier received: {query_type_id}")
                 self.send_error(f"Query failed: Unknown query type identifier: {query_type_id}")
                 return

            # --- Log the raw result from processor --- 
            logger.info(f"HANDLE_QUERY: Result received from processor ({query_type_id}): {result.get('status')}, map_path_present={result.get('map_filepath') is not None}")
            # For debugging, log the first few keys if it's complex
            if isinstance(result, dict):
                keys_sample = list(result.keys())[:5] # Show first 5 keys
                logger.debug(f"HANDLE_QUERY: Result keys sample: {keys_sample}")
            # ----------------------------------------

            if result.get('status') == 'error':
                logger.error(f"Query {query_type_id} error: {result.get('message')}")
                self.send_error(f"Query failed: {result.get('message')}")
                return

            # Prepare response data - START MINIMAL
            response_data = {
                'status': STATUS_OK,
                'query_id': query_id,
                'query_type': query_type_id,
                'title': result.get('title', f'{query_type_id} Results'),
                'message': "Query successful"
                # Intentionally leave out data/headers/map/plot initially
            }

            # Add data/headers ONLY if they exist in the processor result
            if 'data' in result and result['data'] is not None:
                 # Pickle handles the data directly, no encoding needed
                 response_data['data'] = result['data']
            if 'headers' in result:
                 response_data['headers'] = result['headers']

            # Add map_filepath ONLY if it exists in the processor result
            if 'map_filepath' in result and result['map_filepath'] is not None:
                 response_data['map_filepath'] = result['map_filepath']
                 # Ensure no conflicting keys are present
                 response_data.pop('plot', None) 
                 response_data.pop('metadata_type', None)
            # Add plot ONLY if it exists (e.g., for Query 3) AND map_filepath is NOT present
            elif 'plot' in result and result['plot'] is not None:
                 # Send the raw figure object directly - Pickle will handle it
                 response_data['plot'] = result['plot'] # <<< Keep the fig object
                 # Ensure no conflicting keys are present
                 response_data.pop('map_filepath', None)

            # --- Log the response data being sent --- 
            logger.info(f"HANDLE_QUERY: Sending response_data to client. Map path included: {response_data.get('map_filepath') is not None}")
            logger.debug(f"HANDLE_QUERY: response_data keys: {list(response_data.keys())}")
            # ---------------------------------------

            # Send response
            self.send_response(MSG_QUERY_RESULT, response_data)

            # Log to server activity
            self.server.log_activity(f"Query {query_type_id} processed for {self.client_info['nickname']}")
            
            # --- Notify server to trigger potential GUI updates (e.g., All Clients list) ---
            self.server.notify_query_processed(self.client_info['id'])
            # ----------------------------------------------------------------------------------

        except AttributeError as ae:
             logger.error(f"DataProcessor missing method for query '{query_type_id}': {ae}", exc_info=True)
             self.send_error(f"Server error processing query: {query_type_id} method not implemented.")
        except Exception as e:
            logger.error(f"Error processing query {query_type_id}: {e}", exc_info=True)
            self.send_error(f"Query failed: {str(e)}")
    
    def handle_get_metadata(self, data):
        """Handle a metadata request from the client"""
        if not self.client_info: # Check if logged in
             self.send_error("Metadata request failed: not logged in")
             return

        metadata_req_type = data.get('type')
        logger.info(f"Processing metadata request from {self.client_info['nickname']}: {metadata_req_type}")

        metadata_result = None
        try:
            if metadata_req_type == 'areas':
                metadata_result = self.data_processor.get_unique_areas()
            elif metadata_req_type == 'charge_groups':
                metadata_result = self.data_processor.get_unique_charge_groups()
            elif metadata_req_type == 'descent_codes':
                metadata_result = self.data_processor.get_unique_descent_codes()
            elif metadata_req_type == 'date_range':
                 min_date, max_date = self.data_processor.get_date_range()
                 # Convert dates to ISO strings for JSON compatibility
                 metadata_result = {
                     'min_date': min_date.isoformat() if min_date else None,
                     'max_date': max_date.isoformat() if max_date else None
                 }
            elif metadata_req_type == 'arrest_type_codes':
                 metadata_result = self.data_processor.get_unique_arrest_type_codes()
            # ---------------------
            else:
                self.send_error(f"Unknown metadata type requested: {metadata_req_type}")
                return

            # Send the result back (using MSG_QUERY_RESULT structure for convenience)
            response_data = {
                'status': STATUS_OK,
                'metadata_type': metadata_req_type, # Let client know what data this is
                'data': metadata_result
            }
            self.send_response(MSG_QUERY_RESULT, response_data)
            logger.info(f"Sent metadata '{metadata_req_type}' to {self.client_info['nickname']}")

        except AttributeError as ae:
             logger.error(f"DataProcessor missing method for metadata '{metadata_req_type}': {ae}", exc_info=True)
             self.send_error(f"Server error processing metadata request: {metadata_req_type} method not implemented.")
        except Exception as e:
            logger.error(f"Error fetching metadata '{metadata_req_type}': {e}", exc_info=True)
            self.send_error(f"Error fetching metadata: {e}")
    
    def send_response(self, msg_type, data):
        """Send a response to the client"""
        message = Message(msg_type, data)
        send_message(self.socket, message)
    
    def send_error(self, error_message, request_id=None):
        """Send an error response to the client"""
        response_data = {
            'status': STATUS_ERROR,
            'message': error_message
        }
        if request_id:
            response_data['request_id'] = request_id # Add request ID if provided
        self.send_response(msg_type='ERROR', data=response_data)
    
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
    
    def cleanup(self, client_removed_before_cleanup):
        """Clean up resources"""
        if getattr(self, 'cleaned_up', False):
            return
        self.cleaned_up = True
        
        logger.info(f"Cleaning up connection for client {self.address}. Connection lost flag: {self.connection_lost}")
        
        # End session if client was logged in (still necessary if loop exited unexpectedly)
        # Use was_logged_in flag as client_info might be None already
        if self.was_logged_in and self.session_id:
            try:
                self.db.end_session(self.session_id) 
                logger.info(f"Session ended for client ID associated with session {self.session_id}")
                # Log activity only if not removed before cleanup (to avoid duplicate logs)
                if not client_removed_before_cleanup:
                     # Need nickname/email for logging, might need to fetch if self.client_info is None
                     # For now, log generic message if info is gone
                     log_msg_client = f"({self.address})" # Fallback identifier
                     if self.client_info: 
                          log_msg_client = f"{self.client_info.get('nickname', '?')} ({self.client_info.get('email', '?')})"
                     self.server.log_activity(f"Client disconnected (cleanup): {log_msg_client}")
            except Exception as e:
                logger.error(f"Error ending session during cleanup: {e}", exc_info=True)
        
        # Close socket only if connection wasn't already lost
        if not self.connection_lost:
            logger.debug(f"CLEANUP [{self.address}]: Connection not marked as lost, attempting to close socket...")
            try:
                if self.socket:
                    self.socket.close()
                    self.socket = None
                    logger.debug(f"CLEANUP [{self.address}]: Socket closed cleanly.")
                else:
                    logger.debug(f"CLEANUP [{self.address}]: Socket was already None.")
            except Exception as e:
                logger.error(f"CLEANUP [{self.address}]: Error closing socket cleanly: {e}")
        else:
             logger.debug(f"CLEANUP [{self.address}]: Connection marked as lost, skipping socket close.")
             # Set socket to None anyway to prevent potential reuse elsewhere if cleanup logic changes
             self.socket = None 
        
        
        logger.info(f"CLEANUP [{self.address}]: Cleanup finished.")
        
