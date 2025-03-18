# Dataset Query Socket Application

A client-server socket application for dataset analysis and querying, with user authentication and moderator capabilities.

## Features

### Server-side (Moderator):
- Graphical user interface for managing the server
- User registration and authentication
- Track client connections and their information (name, nickname, email)
- Monitor and display client search queries
- View query popularity statistics
- Send broadcast messages to connected clients
- Monitor server logs in real-time

### Client-side:
- Graphical user interface using PySide6 (Qt)
- Registration and login functionality
- Send custom messages to the server
- Execute dataset queries with parameters
- View query results
- Receive broadcast messages from the server
- Track query history

## Requirements

- Python 3.6 or higher
- PySide6 (Qt for Python)
- Socket, threading, and logging modules



## How to Run

### Starting the Enhanced Server with GUI

1. Open a terminal/command prompt
2. Navigate to the project directory
3. Run the server:
   ```
   python Server/server_gui.py
   ```
4. The server moderator GUI will appear
5. Set the host and port (defaults to 127.0.0.1:8888)
6. Click "Start Server" to start listening for connections

### Starting the Enhanced Client

1. Open a new terminal/command prompt
2. Navigate to the project directory
3. Run the client:
   ```
   python Client/client_advanced.py
   ```
4. The client GUI will appear
5. Enter the server IP and port (defaults to 127.0.0.1:8888)
6. Click "Connect" to connect to the server
7. Register a new user account or login with existing credentials
8. Once logged in, you can send messages and execute queries

## Usage

### Server Features

- **Logs Tab**: View real-time server logs
- **Clients Tab**: See a list of connected clients and their information
- **Query Statistics Tab**: View popularity of different query types
- **Client Details Tab**: View detailed information about a specific client and their query history
- **Broadcast Messages**: Send messages to all connected clients

### Client Features

- **Registration**: Create a new user account with name, nickname, email, and password
- **Login**: Authenticate with existing credentials
- **Query Tab**: Select query types, set parameters, and view results
- **Messages Tab**: Send and receive messages to/from the server
- **Query History Tab**: View history of executed queries

## Dataset Integration

This application is designed to work with datasets from Kaggle.com. The server holds the dataset and processes queries from clients, while clients can request different types of analysis without direct access to the dataset.

To integrate your own dataset:
1. Add dataset loading code in the ServerBackend class in server_gui.py
2. Implement query execution logic in the handle_query method
3. Update the query types in the client_advanced.py file to match your dataset's capabilities

## Logs

Both the client and server generate log files:
- `Client/logs/client_[timestamp].log`
- `Server/logs/server_[timestamp].log`

These logs contain information about connections, disconnections, messages, queries, and errors. 