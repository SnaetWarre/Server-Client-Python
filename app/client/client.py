#!/usr/bin/env python3

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
import tempfile
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from shared.constants import *
from shared.protocol import Message, send_message, receive_message

TEMP_DIR = tempfile.gettempdir()
CLIENT_LOG_FILE = os.path.join(TEMP_DIR, 'client_temp.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=CLIENT_LOG_FILE,
    filemode='w'
)
logger = logging.getLogger('client')


class Client:
    def __init__(self, host=SERVER_HOST, port=SERVER_PORT):
        self.host = host
        self.port = port
        self.socket = None
        self.running = False
        self.connected = False
        self.logged_in = False
        self.client_info = None
        
        self.message_queue = queue.Queue()
        
        self._response_events = {}
        self._response_data = {}
        self._response_lock = threading.Lock()
        
        self.on_connection_status_change = None
        self.on_login_status_change = None
        self.on_message_received = None
        self.on_query_result = None
        self.on_error = None
        
        self.receiver_thread = None
    
    def connect(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            
            self.connected = True
            self.running = True
            
            if self.on_connection_status_change:
                self.on_connection_status_change(True)
            
            self.receiver_thread = threading.Thread(target=self.receive_messages)
            self.receiver_thread.daemon = True
            self.receiver_thread.start()
            
            logger.info(f"Connected to server at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to server: {e}", exc_info=True)
            
            if self.on_error:
                self.on_error(f"Error connecting to server: {e}")
            
            logger.info("Connect failed, cleaning up potentially partial connection.")
            if self.socket:
                try:
                    self.socket.close()
                except OSError: pass
            self.socket = None
            self.connected = False
            self.running = False
            
            if self.on_connection_status_change:
                try:
                    self.on_connection_status_change(False)
                except Exception as cb_err:
                    logger.error(f"Error in connection status callback during cleanup: {cb_err}")
            
            return False
    
    def disconnect(self):
        if self.logged_in:
            self.logout()
        
        self.running = False
        
        if self.socket:
            try:
                self.socket.close()
            except Exception as e:
                logger.error(f"Error closing socket: {e}")
        
        self.connected = False
        self.logged_in = False
        self.client_info = None
        
        if self.on_connection_status_change:
            self.on_connection_status_change(False)
        
        logger.info("Disconnected from server")
    
    def receive_messages(self):
        if self.socket:
            pass

        consecutive_errors = 0
        max_consecutive_errors = 5

        while self.running:
            try:
                socket_valid = False
                socket_fileno = -1
                if self.socket:
                    try:
                        socket_fileno = self.socket.fileno()
                        if socket_fileno != -1:
                             socket_valid = True
                    except Exception as sock_err:
                        logger.warning(f"RECEIVER: Error checking socket status before receive: {sock_err}")

                logger.info(f"RECEIVER LOOP: Socket valid check: {socket_valid}, Fileno: {socket_fileno}. Attempting receive...")
                if not socket_valid:
                     logger.error("RECEIVER LOOP: Socket is invalid or None before calling receive_message. Breaking loop.")
                     self.running = False
                     break

                message = receive_message(self.socket)

                if message is None:
                    logger.warning("Connection closed by server (receive_message returned None)")
                    break

                self.process_message(message)
                consecutive_errors = 0
                time.sleep(0.01)

            except socket.timeout:
                pass

            except (ConnectionError, ValueError, OSError, struct.error, json.JSONDecodeError) as e:
                 logger.error(f"Error receiving messages: {e}", exc_info=True)
                 consecutive_errors += 1
                 if consecutive_errors >= max_consecutive_errors or isinstance(e, OSError) and e.errno == 9:
                     logger.error(f"Too many errors or fatal socket error (Errno 9), disconnecting receiver loop.")
                     break
                 time.sleep(0.5)

        if self.connected:
             logger.warning("Receiver loop finished or connection lost, disconnecting client.")
             self.connected = False
             self.logged_in = False
             self.running = False

             if self.on_connection_status_change:
                  self.on_connection_status_change(False)
             if self.on_error:
                  self.on_error("Connection lost or socket error")

    def process_message(self, message):
        logger.info(f"Received message from server: {message.msg_type}, data keys: {list(message.data.keys()) if isinstance(message.data, dict) else 'N/A'}")

        signaled_event = False
        request_id = message.data.get('request_id')
        if request_id:
            event_to_signal = None
            with self._response_lock:
                if request_id in self._response_events:
                    event_to_signal = self._response_events.pop(request_id)
                    self._response_data[request_id] = message.data
                    signaled_event = True

            if event_to_signal:
                logger.debug(f"Signaling event for request_id: {request_id}")
                event_to_signal.set()

        try:
            if message.msg_type == MSG_LOGIN:
                self.handle_login_response(message.data)
            elif message.msg_type == MSG_LOGOUT:
                self.handle_logout_response(message.data)
            elif message.msg_type == MSG_QUERY_RESULT:
                if not signaled_event:
                    processed_data = message.data.copy()

                    if self.on_query_result:
                        logger.info(f"PROCESS_MESSAGE (Query Result - Async/Metadata): Calling on_query_result callback. Processed keys: {list(processed_data.keys())}")
                        self.on_query_result(processed_data)

                    query_type = processed_data.get('query_type', 'unknown')
                    metadata_type = processed_data.get('metadata_type', 'N/A')
                    log_identifier = metadata_type if metadata_type != 'N/A' else query_type
                    logger.info(f"Received successful query/metadata result for {log_identifier}")
                else:
                    logger.debug(f"Skipping further processing for {message.msg_type} as event was signaled for request_id {request_id}")

            elif message.msg_type == MSG_SERVER_MESSAGE:
                self.handle_server_message(message.data)
            elif message.msg_type == 'ERROR':
                self.handle_error(message.data)
            else:
                if not signaled_event:
                    logger.warning(f"Unknown message type: {message.msg_type}")
                    self.message_queue.put({
                        'type': 'info',
                        'timestamp': datetime.now().isoformat(),
                        'message': f"Received unknown message type: {message.msg_type}"
                    })

        except Exception as e:
            logger.error(f"Error processing message body for type {message.msg_type}: {e}", exc_info=True)
            self.message_queue.put({
                'type': 'error',
                'timestamp': datetime.now().isoformat(),
                'message': f"Error processing message: {e}"
            })
    
    def handle_login_response(self, data):
        status = data.get('status')
        
        if status == STATUS_OK:
            self.logged_in = True
            self.client_info = data.get('client_info')
            
            if self.on_login_status_change:
                self.on_login_status_change(True)
            
            logger.info(f"Logged in as {self.client_info['nickname']}")
            
            self.message_queue.put({
                'type': 'info',
                'timestamp': datetime.now().isoformat(),
                'message': f"Logged in as {self.client_info['nickname']}"
            })
        else:
            error_message = data.get('message', 'Login failed')
            
            if self.on_error:
                self.on_error(error_message)
            
            logger.error(f"Login failed: {error_message}")
            
            self.message_queue.put({
                'type': 'error',
                'timestamp': datetime.now().isoformat(),
                'message': f"Login failed: {error_message}"
            })
    
    def handle_logout_response(self, data):
        status = data.get('status')
        
        if status == STATUS_OK:
            self.logged_in = False
            self.client_info = None
            
            if self.on_login_status_change:
                self.on_login_status_change(False)
            
            logger.info("Logged out")
            
            self.message_queue.put({
                'type': 'info',
                'timestamp': datetime.now().isoformat(),
                'message': "Logged out"
            })
        else:
            error_message = data.get('message', 'Logout failed')
            
            if self.on_error:
                self.on_error(error_message)
            
            logger.error(f"Logout failed: {error_message}")
            
            self.message_queue.put({
                'type': 'error',
                'timestamp': datetime.now().isoformat(),
                'message': f"Logout failed: {error_message}"
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
        
        callback_success = False
        if self.on_message_received:
            try:
                self.on_message_received(timestamp, message_text)
                logger.info("Successfully called on_message_received callback")
                callback_success = True
            except Exception as e:
                logger.error(f"Error in on_message_received callback: {e}")
        
        if not callback_success:
            logger.info("Using message queue for server message")
            self.message_queue.put({
                'type': 'server',
                'timestamp': timestamp,
                'message': message_text,
                'use_queue': True
            })
    
    def handle_error(self, data):
        """Handle error message from the server"""
        error_message = data.get('message', 'Unknown error')
        request_id = data.get('request_id')

        signaled_event = False
        if request_id:
             event_to_signal = None
             with self._response_lock:
                  if request_id in self._response_events:
                       event_to_signal = self._response_events.pop(request_id)
                       self._response_data[request_id] = data
                       signaled_event = True

             if event_to_signal:
                  logger.debug(f"Signaling event for ERROR response (request_id: {request_id})")
                  event_to_signal.set()

        if self.on_error:
            self.on_error(error_message)

        logger.error(f"Server error: {error_message} (Request ID: {request_id or 'N/A'})")
    
    def register(self, name, nickname, email, password):
        """Register a new user"""
        if not self.connected:
            logger.error("Not connected to server")
            return False
        
        data = {
            'name': name,
            'nickname': nickname,
            'email': email,
            'password': password
        }
        request_id = str(uuid.uuid4())
        data['request_id'] = request_id
        
        request_data = Message(MSG_REGISTER, data)
        
        logger.info(f"Sending registration request: Type={request_data.msg_type}, Payload={request_data.data}")
        success = False
        try:
            if self.connected and self.socket:
                success = send_message(self.socket, request_data)
                if not success:
                    logger.error("send_message returned False for registration request.")
            else:
                logger.error("Cannot send registration: Socket not connected.")
        except Exception as e:
            logger.error(f"Exception sending registration request: {e}", exc_info=True)
            self.connected = False
            self.running = False
            if self.on_connection_status_change:
                self.on_connection_status_change(False)
        
        if not success:
            return False, "Failed to send registration request."
        
        response_key = request_id
        event = threading.Event()
        with self._response_lock:
            self._response_events[response_key] = event
            if response_key in self._response_data:
                del self._response_data[response_key]
        
        logger.info(f"Waiting for registration response...")
        event_set = event.wait(timeout=10.0)
        
        response_data = None
        with self._response_lock:
            if response_key in self._response_events:
                 del self._response_events[response_key]
                 logger.warning("Registration response event removed after timeout.")

            if response_key in self._response_data:
                response_data = self._response_data.pop(response_key)
        
        if not event_set or not response_data:
            logger.error("Registration response timed out or data missing.")
            return False, "Registration timed out or server did not respond correctly."
        
        if response_data.get('status') == STATUS_OK:
            logger.info(f"Registration successful (Response received): {response_data.get('message')}")
            return True, response_data.get('message', "Registration successful.")
        else:
            error_msg = response_data.get('message', "Registration failed: Unknown error")
            logger.warning(f"Registration failed (Response received): {error_msg}")
            return False, error_msg
    
    def login(self, email, password):
        """Log in to the server"""
        if not self.connected:
            logger.error("Not connected to server")
            return False
        
        if self.logged_in:
            logger.warning("Already logged in")
            return False
        
        message = Message(MSG_LOGIN, {
            'email': email,
            'password': password
        })
        
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
        
        message = Message(MSG_LOGOUT, {})
        
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
            if self.on_error:
                self.on_error("Cannot send request: Not connected")
            return False

        command = request_data.get('command')
        if not command:
            logger.error("Cannot send request: 'command' key missing in request data")
            if self.on_error:
                self.on_error("Internal error: Command missing in request")
            return False

        msg_type = None
        if command == 'query':
            msg_type = MSG_QUERY
            if not self.logged_in:
                 logger.error("Cannot send query: Not logged in")
                 if self.on_error:
                      self.on_error("Cannot send query: Not logged in")
                 return False
        elif command == 'get_metadata':
            msg_type = MSG_GET_METADATA
            if not self.logged_in:
                 logger.error("Cannot get metadata: Not logged in")
                 if self.on_error:
                      self.on_error("Cannot get metadata: Not logged in")
                 return False
        else:
            logger.error(f"Cannot send request: Unknown command '{command}'")
            if self.on_error:
                 self.on_error(f"Internal error: Unknown command '{command}'")
            return False

        payload = request_data.copy()
        del payload['command']

        message = Message(msg_type, payload)

        logger.info(f"Sending request: Type={msg_type}, Payload={payload}")
        try:
            success = send_message(self.socket, message)
            if not success:
                logger.error(f"Failed to send request (command: {command}) using send_message.")
                if self.on_error:
                     self.on_error(f"Failed to send request for command '{command}'")
                return False
            return True
        except Exception as e:
             logger.error(f"Exception sending request (command: {command}): {e}", exc_info=True)
             if self.on_error:
                  self.on_error(f"Network error sending request: {e}")
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
    client = Client()
    
    try:
        if client.connect():
            print("Connected to server")
            
            while True:
                cmd = input("Enter command (r=register, l=login, o=logout, q=query, x=exit): ")
                
                if cmd == 'r':
                    name = input("Name: ")
                    nickname = input("Nickname: ")
                    email = input("Email: ")
                    password = input("Password: ")
                    
                    client.register(name, nickname, email, password)
                elif cmd == 'l':
                    email = input("Email: ")
                    password = input("Password: ")
                    
                    client.login(email, password)
                elif cmd == 'o':
                    client.logout()
                elif cmd == 'q':
                    if client.logged_in:
                        query_type = input("Query type: ")
                        
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