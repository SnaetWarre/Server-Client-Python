#!/usr/bin/env python3
# Protocol for client-server communication

import json
import socket
import struct
import pickle
import base64

class Message:
    """
    Message class for communication between client and server
    """
    def __init__(self, msg_type, data=None):
        self.msg_type = msg_type
        self.data = data if data is not None else {}
    
    def to_json(self):
        """Convert message to JSON string"""
        msg_dict = {
            'msg_type': self.msg_type,
            'data': self.data
        }
        return json.dumps(msg_dict)
    
    @classmethod
    def from_json(cls, json_str):
        """Create message from JSON string"""
        msg_dict = json.loads(json_str)
        return cls(msg_dict['msg_type'], msg_dict['data'])


def send_message(sock, message):
    """
    Send a message object through a socket
    
    Parameters:
    - sock: socket object
    - message: Message object
    
    Returns:
    - True if message sent successfully, False otherwise
    """
    try:
        # Convert message to JSON
        msg_json = message.to_json()
        
        # Convert JSON to bytes
        msg_bytes = msg_json.encode('utf-8')
        
        # Send message length first (4 bytes, network byte order)
        msg_len = len(msg_bytes)
        sock.sendall(struct.pack('!I', msg_len))
        
        # Send the message itself
        sock.sendall(msg_bytes)
        
        return True
    except socket.timeout:
        print(f"Timeout sending message: {message.msg_type}")
        raise
    except ConnectionError as ce:
        print(f"Connection error sending message: {message.msg_type} - {ce}")
        raise
    except Exception as e:
        print(f"Error sending message: {message.msg_type} - {e}")
        return False


def receive_message(sock):
    """
    Receive a message object from a socket
    
    Parameters:
    - sock: socket object
    
    Returns:
    - Message object if received successfully, None otherwise
    - Raises socket.timeout if a timeout occurs (which should be caught by the caller)
    """
    try:
        # Receive message length first (4 bytes, network byte order)
        msg_len_bytes = sock.recv(4)
        if not msg_len_bytes:
            return None
        
        msg_len = struct.unpack('!I', msg_len_bytes)[0]
        
        # Sanity check on message size to prevent excessive memory allocation
        if msg_len > 10 * 1024 * 1024:  # Limit to 10MB
            print(f"Message size too large: {msg_len} bytes")
            return None
        
        # Receive the message itself
        data = b''
        while len(data) < msg_len:
            packet = sock.recv(min(msg_len - len(data), 4096))
            if not packet:
                return None
            data += packet
        
        # Convert bytes to JSON
        msg_json = data.decode('utf-8')
        
        # Create Message object from JSON
        message = Message.from_json(msg_json)
        
        return message
    except socket.timeout:
        # Re-raise timeout exceptions to be handled by the caller
        raise
    except ConnectionError as ce:
        # Re-raise connection errors to be handled by the caller
        print(f"Connection error receiving message: {ce}")
        raise
    except json.JSONDecodeError as je:
        print(f"JSON decode error: {je}")
        return None
    except Exception as e:
        print(f"Error receiving message: {e}")
        return None


def encode_dataframe(df):
    """Encode DataFrame for transmission"""
    return base64.b64encode(pickle.dumps(df)).decode('utf-8')


def decode_dataframe(encoded_df):
    """Decode DataFrame after transmission"""
    return pickle.loads(base64.b64decode(encoded_df.encode('utf-8')))


def encode_figure(fig):
    """Encode matplotlib figure for transmission"""
    import io
    import matplotlib.pyplot as plt
    
    # Save figure to a bytes buffer
    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    
    # Encode the buffer as base64
    encoded = base64.b64encode(buf.read()).decode('utf-8')
    plt.close(fig)
    return encoded


def decode_figure(encoded_fig):
    """Decode matplotlib figure after transmission"""
    import io
    import matplotlib.pyplot as plt
    from PIL import Image
    
    # Decode the base64 string
    decoded = base64.b64decode(encoded_fig.encode('utf-8'))
    
    # Create a bytes buffer
    buf = io.BytesIO(decoded)
    
    # Open the image with PIL
    img = Image.open(buf)
    return img 