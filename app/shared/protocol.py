#!/usr/bin/env python3
# Protocol for client-server communication

import json
import socket
import struct
import pickle
import base64
import logging
import os

# Get the logger for this module
logger = logging.getLogger(__name__) # Use module-level logger

class Message:
    """
    Message class for communication between client and server
    """
    def __init__(self, msg_type, data=None):
        self.msg_type = msg_type
        self.data = data if data is not None else {}


def send_message(sock, message):
    """
    Send a message object through a socket using Pickle
    
    Parameters:
    - sock: socket object
    - message: Message object
    
    Returns:
    - True if message sent successfully, False otherwise
    """
    try:
        # Convert message object to bytes using pickle
        msg_bytes = pickle.dumps(message, protocol=4) # Use a high protocol
        
        # Send message length first (4 bytes, network byte order)
        msg_len = len(msg_bytes)
        sock.sendall(struct.pack('!I', msg_len))
        
        # Send the message itself
        sock.sendall(msg_bytes)
        
        return True
    except socket.timeout:
        logger.warning(f"Timeout sending message: {message.msg_type}") # Use logger
        raise
    except ConnectionError as ce:
        logger.error(f"Connection error sending message: {message.msg_type} - {ce}") # Use logger
        raise
    except Exception as e:
        logger.error(f"Error sending message: {message.msg_type} - {e}", exc_info=True) # Use logger and add traceback
        return False


def receive_message(sock):
    """
    Receive a message object from a socket using Pickle
    
    Parameters:
    - sock: socket object
    
    Returns:
    - Message object if received successfully, None if connection closed gracefully
    - Raises socket.timeout, ConnectionError, or other exceptions on error
    """
    fileno = -1 # For logging purposes if needed
    try:
        # Basic check if socket seems valid before recv
        if sock is None:
             raise TypeError("Invalid object (None) passed as socket to receive_message")
        fileno = sock.fileno()
        if fileno == -1:
             # This case should ideally not happen if the socket object exists,
             # but check anyway.
             raise OSError(9, 'Bad file descriptor (-1) passed to protocol.receive_message')

        # --- Receive message length (4 bytes) ---
        # Use a reasonable timeout to avoid indefinite blocking
        original_timeout = sock.gettimeout()
        sock.settimeout(2.0)
        try:
            msg_len_bytes = sock.recv(4)
        finally:
            sock.settimeout(original_timeout) # Restore original timeout

        if not msg_len_bytes:
            # Connection closed gracefully by peer
            logger.info(f"PROTOCOL.RECEIVE: Connection closed gracefully by peer (socket fileno {fileno}) before length received.")
            return None

        msg_len = struct.unpack('!I', msg_len_bytes)[0]

        # --- Sanity check on message size ---
        MAX_MSG_SIZE = 20 * 1024 * 1024 # Increased limit to 20MB, adjust if needed
        if msg_len > MAX_MSG_SIZE:
            logger.error(f"PROTOCOL.RECEIVE: Message size {msg_len} bytes exceeds limit {MAX_MSG_SIZE} (socket fileno {fileno}). Closing socket.")
            try:
                 sock.close() # Attempt to close our end
            except Exception as close_err:
                 logger.warning(f"Ignoring error while closing socket after size limit exceeded: {close_err}")
            raise ValueError(f"Received message size ({msg_len}) exceeds limit.")

        # --- Receive the message body ---
        data = b''
        bytes_received = 0
        # Use a longer timeout for the body, proportionate to max size?
        body_timeout = max(30.0, MAX_MSG_SIZE / (1024*1024) * 2) # e.g., 2s per MB, min 30s
        sock.settimeout(body_timeout)
        logger.debug(f"PROTOCOL.RECEIVE: Expecting {msg_len} bytes for message body (timeout: {body_timeout}s)...")
        try:
            while bytes_received < msg_len:
                chunk_size = min(msg_len - bytes_received, 8192) # Read in larger chunks
                packet = sock.recv(chunk_size)
                if not packet:
                    logger.warning(f"PROTOCOL.RECEIVE: Connection closed unexpectedly while receiving message body (received {bytes_received}/{msg_len} bytes, socket fileno {fileno}).")
                    raise ConnectionError("Connection closed during message body reception")
                data += packet
                bytes_received += len(packet)
        finally:
            sock.settimeout(original_timeout) # Restore original timeout

        logger.debug(f"PROTOCOL.RECEIVE: Received {bytes_received} bytes for message body.")

        # Deserialize bytes using pickle
        message = pickle.loads(data)
        
        if not isinstance(message, Message):
            logger.error(f"PROTOCOL.RECEIVE: Deserialized object is not a Message type ({type(message)}). Socket {fileno}")
            raise TypeError("Received invalid object type from socket")

        logger.debug(f"PROTOCOL.RECEIVE: Successfully received message type {message.msg_type} (socket fileno {fileno}).")
        return message

    except socket.timeout:
        logger.warning(f"PROTOCOL.RECEIVE: Socket timeout during receive (socket fileno {fileno}).")
        raise # Re-raise timeout
    except (ConnectionError, ValueError, struct.error, json.JSONDecodeError, OSError) as e:
        # Catch specific, expected errors and OSError
        logger.error(f"PROTOCOL.RECEIVE: Error receiving/decoding message: {type(e).__name__} - {e} (socket fileno {fileno})")
        raise # Re-raise these specific errors
    except Exception as e:
         # Catch any other unexpected errors
         logger.error(f"PROTOCOL.RECEIVE: Unexpected error receiving message: {type(e).__name__} - {e} (socket fileno {fileno})", exc_info=True)
         raise # Re-raise
