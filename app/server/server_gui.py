#!/usr/bin/env python3
# GUI for the server application (using PySide6)

import os
import sys
import time
import datetime
import tempfile
import logging
import pandas as pd

# Add the parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from .server import Server
from .gui_stylingsheets import DARK_STYLESHEET, LIGHT_STYLESHEET

from PySide6.QtWidgets import (
    QMainWindow, QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QLineEdit, QTextEdit, QTabWidget, QTreeWidget, QTreeWidgetItem,
    QGroupBox, QMessageBox, QSplitter, QStatusBar, QFormLayout, QFrame, QCheckBox,
    QTableWidget, QTableWidgetItem
)
from PySide6.QtCore import Qt, QTimer, Signal, QSettings
from PySide6.QtGui import QFont, QPalette, QColor, QIntValidator
from matplotlib.figure import Figure
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
import matplotlib.dates as mdates

TEMP_DIR_GUI = tempfile.gettempdir()
SERVER_GUI_LOG_FILE = os.path.join(TEMP_DIR_GUI, 'server_gui_temp.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=SERVER_GUI_LOG_FILE, 
    filemode='w'                  
)
logger = logging.getLogger('server_gui')

def format_timestamp(ts_str, default="Unknown"):
    """Helper to format ISO timestamp strings nicely, handling None."""
    if not ts_str:
        return default
    try:
        # Parse ISO format potentially including timezone
        dt = datetime.datetime.fromisoformat(ts_str.replace('Z', '+00:00')) 
        # Convert to local timezone for display
        local_dt = dt.astimezone()
        return local_dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        # Fallback for potentially different formats or non-string types
        return str(ts_str) 

class ServerGUI(QMainWindow):
    """Main window for the Server application.
    Handles the user interface for controlling the server, managing clients,
    viewing activity logs, and displaying statistics.
    """
    
    # Thread-safe Qt signals for cross-thread UI updates
    activity_log_signal = Signal(str, str)
    client_list_update_signal = Signal()
    all_clients_list_update_signal = Signal() # Signal for all clients list
    
    def __init__(self):
        """Initialize the GUI, set up settings, signals, and initial UI state."""
        super().__init__()
        
        self.setWindowTitle("Arrest Data Server")
        self.resize(900, 700)
        self.setMinimumSize(800, 600)
        

        self.settings = QSettings("ArrestDataApp", "Server/AppSettings")
        self.dark_theme = self.settings.value("dark_theme", True, type=bool)
        
        # Server instance - Initialize to None, created on start
        self.server = None
        self.server_running = False # Explicitly set running state
        self.query_stats_labels = {} # Dictionary to store labels for query stats
        self.currently_displayed_client_id = None # Track ID for details view
        
        # Hook our Qt signals into the GUI slots
        self.activity_log_signal.connect(self.on_activity_log)
        self.client_list_update_signal.connect(self.update_client_list)
        self.all_clients_list_update_signal.connect(self.update_all_clients)

        self.setup_gui()
    
        self.apply_theme()    
        
        self.update_timer = QTimer(self)
        self.update_timer.timeout.connect(self.schedule_updates)
        self.update_timer.start(1000) #this is in miliseconds 
    
    def setup_gui(self):
        """Set up the main GUI layout including tabs, status bar, and theme toggle."""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        self.tab_widget = QTabWidget()
        main_layout.addWidget(self.tab_widget)

        self.server_control_tab = QWidget()
        self.messaging_tab = QWidget()
        self.client_management_tab = QWidget()
        self.statistics_tab = QWidget()

        self.tab_widget.addTab(self.server_control_tab, "Server Control")
        self.tab_widget.addTab(self.messaging_tab, "Messaging")
        self.tab_widget.addTab(self.client_management_tab, "Client Management")
        self.tab_widget.addTab(self.statistics_tab, "Statistics")

        self.setup_server_control_tab_content(self.server_control_tab)
        self.setup_messaging_tab_content(self.messaging_tab)
        self.setup_client_management_tab_content(self.client_management_tab)
        self.setup_statistics_tab_content(self.statistics_tab)

        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Server Idle")

        self.theme_button = QPushButton("Toggle Theme")
        self.theme_button.setCheckable(True)
        self.theme_button.setChecked(self.dark_theme)
        self.theme_button.clicked.connect(self.toggle_theme)

        theme_layout = QHBoxLayout()
        theme_layout.addStretch()
        theme_layout.addWidget(self.theme_button)
        main_layout.addLayout(theme_layout)

    def apply_theme(self):
        """Apply the current theme to the application"""
        if self.dark_theme:
            QApplication.instance().setStyleSheet(DARK_STYLESHEET)
        else:
            QApplication.instance().setStyleSheet(LIGHT_STYLESHEET)
    
    def toggle_theme(self):
        """Toggle between dark and light themes."""
        self.dark_theme = not self.dark_theme
        self.settings.setValue("dark_theme", self.dark_theme)
        self.apply_theme()
        self.theme_button.setChecked(self.dark_theme) # Update button state
    
    def setup_server_control_tab_content(self, tab):
        """Sets up the content for the Server Control tab."""
        layout = QVBoxLayout(tab)

        server_control_group = QGroupBox("Server Control")
        server_control_layout = QHBoxLayout()
        server_control_group.setLayout(server_control_layout)

        host_port_layout = QFormLayout()
        self.host_input = QLineEdit(self.settings.value("server_host", "127.0.0.1"))
        self.port_input = QLineEdit(self.settings.value("server_port", defaultValue="8888", type=str))
        self.port_input.setValidator(QIntValidator(1024, 65535)) # Basic port validation
        host_port_layout.addRow("Host:", self.host_input)
        host_port_layout.addRow("Port:", self.port_input)
        server_control_layout.addLayout(host_port_layout)

        button_layout = QVBoxLayout()
        self.start_button = QPushButton("Start Server")
        self.start_button.clicked.connect(self.start_server)
        self.stop_button = QPushButton("Stop Server")
        self.stop_button.clicked.connect(self.stop_server)
        self.stop_button.setEnabled(False) 
        button_layout.addWidget(self.start_button)
        button_layout.addWidget(self.stop_button)
        server_control_layout.addLayout(button_layout)
        server_control_layout.addStretch()

        layout.addWidget(server_control_group)

        # Activity Log Group (reuse existing log area)
        activity_log_group = QGroupBox("Activity Log")
        log_layout = QVBoxLayout()
        activity_log_group.setLayout(log_layout)

        self.activity_log = QTextEdit()
        self.activity_log.setReadOnly(True)
        self.activity_log.setFont(QFont("Courier", 10))
        log_layout.addWidget(self.activity_log)

        layout.addWidget(activity_log_group)
        tab.setLayout(layout)

    def setup_messaging_tab_content(self, tab):
        """Sets up the content for the Messaging tab."""
        layout = QVBoxLayout(tab)
        splitter = QSplitter(Qt.Vertical) # Split active clients and messaging

        active_clients_group = QGroupBox("Active Clients")
        active_clients_layout = QVBoxLayout()
        active_clients_group.setLayout(active_clients_layout)

        self.active_clients_tree = QTreeWidget()
        self.active_clients_tree.setHeaderLabels(["Client ID", "Nickname", "IP Address", "Connected At"])
        self.active_clients_tree.itemSelectionChanged.connect(self.on_client_selected)
        active_clients_layout.addWidget(self.active_clients_tree)
        splitter.addWidget(active_clients_group)

        messaging_area = QWidget()
        messaging_layout = QVBoxLayout(messaging_area)

        direct_message_group = QGroupBox("Send Message to Selected Client")
        direct_message_layout = QHBoxLayout()
        direct_message_group.setLayout(direct_message_layout)
        self.client_message_input = QLineEdit()
        self.client_message_input.setPlaceholderText("Type message here...")
        self.send_client_button = QPushButton("Send")
        self.send_client_button.clicked.connect(self.send_client_message)
        self.send_client_button.setEnabled(False) # Disable until client selected
        direct_message_layout.addWidget(self.client_message_input)
        direct_message_layout.addWidget(self.send_client_button)
        messaging_layout.addWidget(direct_message_group)

        broadcast_group = QGroupBox("Send Broadcast Message")
        broadcast_layout = QHBoxLayout()
        broadcast_group.setLayout(broadcast_layout)
        self.broadcast_input = QLineEdit()
        self.broadcast_input.setPlaceholderText("Type broadcast message here...")
        self.broadcast_button = QPushButton("Broadcast")
        self.broadcast_button.clicked.connect(self.broadcast_message)
        broadcast_layout.addWidget(self.broadcast_input)
        broadcast_layout.addWidget(self.broadcast_button)
        messaging_layout.addWidget(broadcast_group)

        splitter.addWidget(messaging_area)
        layout.addWidget(splitter)
        tab.setLayout(layout)

    def setup_client_management_tab_content(self, tab):
        """Sets up the content for the Client Management tab."""
        layout = QVBoxLayout(tab)
        splitter = QSplitter(Qt.Vertical)

        all_clients_group = QGroupBox("All Registered Clients")
        all_clients_layout = QVBoxLayout()
        all_clients_group.setLayout(all_clients_layout)
        self.all_clients_table = QTableWidget()
        self.all_clients_table.setColumnCount(5)
        self.all_clients_table.setHorizontalHeaderLabels(["ID", "Username", "Registered At", "Last Seen", "Total Queries"])
        
        self.all_clients_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.all_clients_table.setSelectionMode(QTableWidget.SingleSelection)
        self.all_clients_table.setAlternatingRowColors(True)
        self.all_clients_table.verticalHeader().setVisible(False)
        self.all_clients_table.setEditTriggers(QTableWidget.NoEditTriggers)
        
        self.all_clients_table.itemSelectionChanged.connect(self.on_all_client_selected)
        all_clients_layout.addWidget(self.all_clients_table)
        splitter.addWidget(all_clients_group)

        details_group = QGroupBox("Client Details")
        details_layout = QVBoxLayout()
        details_group.setLayout(details_layout)
        self.client_details_area = QTextEdit()
        self.client_details_area.setReadOnly(True)
        details_layout.addWidget(self.client_details_area)
        splitter.addWidget(details_group)

        layout.addWidget(splitter)
        tab.setLayout(layout)

    def setup_statistics_tab_content(self, tab):
        """Sets up the content for the Statistics tab."""
        layout = QVBoxLayout(tab)

        stats_group = QGroupBox("Server Statistics")
        stats_layout = QFormLayout() # Use form layout for key-value pairs
        stats_group.setLayout(stats_layout)

        self.uptime_label = QLabel("Calculating...")
        self.registered_clients_label = QLabel("0")
        self.total_queries_label = QLabel("0")


        stats_layout.addRow("Server Uptime:", self.uptime_label)
        stats_layout.addRow("Total Registered Clients:", self.registered_clients_label)
        stats_layout.addRow("Total Queries Processed:", self.total_queries_label)

        layout.addWidget(stats_group)
        layout.addStretch()
        tab.setLayout(layout)

        self.stats_plot_group = QGroupBox("Daily Query Trends")
        plot_layout = QVBoxLayout(self.stats_plot_group)
        plot_layout.setContentsMargins(2, 5, 2, 2)

        # Create an initial empty figure
        initial_fig = Figure(figsize=(8, 4), dpi=100)
        self.stats_canvas = FigureCanvasQTAgg(initial_fig)
        # Add the canvas to the plot layout
        plot_layout.addWidget(self.stats_canvas)

        layout.addWidget(self.stats_plot_group)
        layout.setStretchFactor(stats_group, 1)
        layout.setStretchFactor(self.stats_plot_group, 3) 

    def start_server(self):
        """Creates a Server instance and starts it in a separate thread.
        Handles input validation for host/port, saves these settings,
        registers server event callbacks, and updates the UI to reflect the running state.
        """
        # Prevent starting if already running (or if server object exists)
        if self.server_running:
            QMessageBox.warning(self, "Warning", "Server is already running.")
            return

        host = self.host_input.text().strip()
        port_str = self.port_input.text().strip()

        if not host:
            QMessageBox.warning(self, "Error", "Host cannot be empty.")
            return

        try:
            port = int(port_str)
            if not (1024 <= port <= 65535):
                raise ValueError("Port out of range")
        except ValueError:
            QMessageBox.warning(self, "Error", "Invalid Port number. Must be between 1024 and 65535.")
            return

        self.settings.setValue("server_host", host)
        self.settings.setValue("server_port", port)

        try:
            self.server = Server(host=host, port=port)

            self.server.on_activity_log = (
                lambda timestamp, message: self.activity_log_signal.emit(timestamp, message)
            )
            self.server.on_client_list_update = (
                lambda: self.client_list_update_signal.emit()
            )
            self.server.on_all_clients_update = (
                lambda: self.all_clients_list_update_signal.emit()
            )
            self.server.start()

            self.status_bar.showMessage(f"Server started on {host}:{port}")
            self.server_running = True
            self.start_button.setEnabled(False)
            self.stop_button.setEnabled(True)
            self.host_input.setEnabled(False)
            self.port_input.setEnabled(False)

            self.update_all_clients()

        except Exception as e:
            self.server = None
            self.server_running = False
            QMessageBox.critical(self, "Server Start Error", f"Failed to create or start server: {e}")
            self.status_bar.showMessage("Server failed to start.")
            self.start_button.setEnabled(True)
            self.stop_button.setEnabled(False)
            self.host_input.setEnabled(True)
            self.port_input.setEnabled(True)
    
    def stop_server(self):
        """Stops the server gracefully, updates the UI to a stopped state,
        and clears any server-related data from the GUI (client lists, details).
        """
        # Check if we think the server is running (based on GUI state)
        if self.server_running:
            logger.info("Attempting to stop server...")
            if self.server.stop(): # stop() should return True/False
                 self.status_bar.showMessage("Server stopped.")
            else:
                self.status_bar.showMessage("Failed to stop server gracefully.")
                QMessageBox.warning(self, "Stop Error", "Server did not stop cleanly. Check logs.")

            logger.info("Resetting GUI state to stopped.")
            self.server = None
            self.server_running = False
            self.start_button.setEnabled(True)
            self.stop_button.setEnabled(False)
            self.host_input.setEnabled(True)
            self.port_input.setEnabled(True)

            self.active_clients_tree.clear()
            self.all_clients_table.clear()
            self.client_details_area.clear()

            self.update_statistics()

        elif not self.server_running:
             QMessageBox.information(self, "Info", "Server is not running.")
             self.start_button.setEnabled(True)
             self.stop_button.setEnabled(False)
             self.host_input.setEnabled(True)
             self.port_input.setEnabled(True)
    
    def broadcast_message(self):
        """Sends a broadcast message to all connected clients."""
        message = self.broadcast_input.text().strip()
        if message:
            if self.server.broadcast_message(message):
                self.broadcast_input.clear()
                QMessageBox.information(self, "Message", "Message broadcast to all clients!")
            else:
                QMessageBox.warning(self, "Message", "No clients connected to broadcast to!")
        else:
            QMessageBox.warning(self, "Message", "Please enter a message to broadcast!")
    
    def send_client_message(self):
        """Sends a direct message from the input field to the currently selected client."""
        client_id = getattr(self, 'selected_active_client_id', None)
        if client_id is None:
             QMessageBox.warning(self, "Error", "No client selected or client ID missing.")
             return

        message_text = self.client_message_input.text()
        if not message_text:
            QMessageBox.warning(self, "Error", "Message cannot be empty")
            return
        logger.info(f"Attempting to send message to client ID: {client_id}")
        success = self.server.send_message_to_client(client_id, message_text)

        if success:
            self.client_message_input.clear()
            self.status_bar.showMessage(f"Message sent to Client {client_id}", 3000)
            self.active_clients_tree.clearSelection()
            self.send_client_button.setEnabled(False)
            self.client_message_input.setPlaceholderText("Select a client first")
            self.selected_active_client_id = None
        else:
            QMessageBox.warning(self, "Error", "Failed to send message to client")
    
    def on_activity_log(self, timestamp, message):
        """Handles incoming activity log messages from the server, formats them,
        and appends them to the activity log display area.
        """
        try:
            dt = datetime.datetime.fromisoformat(timestamp)
            formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            formatted_time = timestamp
        
        log_entry = f"[{formatted_time}] {message}"
        
        self.activity_log.append(log_entry)
        scrollbar = self.activity_log.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
    
    def update_client_list(self):
        """Callback function to trigger an update of the active clients list display."""
        if self.server:
            self.update_active_clients()
        else:
            self.active_clients_tree.clear()
    
    def update_active_clients(self):
        """Updates the tree widget displaying actively connected clients.
        It attempts to preserve the selection if the previously selected client is still active.
        """
        if not self.server:
            logger.warning("update_active_clients called but self.server is None.")
            self.active_clients_tree.clear()
            return

        # 1) Remember selected client ID (or address as fallback)
        prev_selected_id = None
        sel = self.active_clients_tree.selectedItems()
        if sel:
            # Try getting ID first, fallback to address if ID wasn't stored previously
            prev_selected_id = sel[0].data(0, Qt.UserRole) or sel[0].text(1) # ID in UserRole, Address in Col 1
        
        # 2) Rebuild the tree
        self.active_clients_tree.clear()
        active_clients = self.server.get_active_clients()
        item_to_restore = None
        
        for client in active_clients:
            client_id = client.get("id")
            address = client.get("address", "Unknown")
            nickname = client.get("nickname", "N/A")
            connected_at = format_timestamp(client.get("connected_since"))

            # Create tree item with correct columns
            item = QTreeWidgetItem([str(client_id) if client_id else "N/A", nickname, address, connected_at])
            # Store the actual client ID in the item's data role
            item.setData(0, Qt.UserRole, client_id)
            self.active_clients_tree.addTopLevelItem(item)

            current_identifier = client_id or address # Use ID if available, else address
            if current_identifier == prev_selected_id:
                item_to_restore = item
        
        # NOTE: self.registered_clients_label is updated in update_statistics now
        # 4) Reselect the same client if it still exists
        if item_to_restore:
            # Block signals briefly to prevent re-triggering on_client_selected
            self.active_clients_tree.blockSignals(True)
            self.active_clients_tree.setCurrentItem(item_to_restore)
            self.active_clients_tree.blockSignals(False)
            # Manually call on_client_selected to re-enable send button and set ID
            self.on_client_selected(item_to_restore)
    
    def update_all_clients(self):
        """Updates the table widget displaying all registered clients.
        It fetches the latest client data from the server and refreshes the table,
        attempting to preserve the selection if the previously selected client still exists.
        """
        logger.info("Updating 'All Clients' table...")
        try:
            # --- Remember selection --- 
            selected_client_id = None
            current_selection = self.all_clients_table.selectedItems()
            if current_selection:
                selected_row = self.all_clients_table.row(current_selection[0])
                id_item = self.all_clients_table.item(selected_row, 0) # ID is column 0
                if id_item:
                    try:
                        selected_client_id = int(id_item.text())
                    except (ValueError, TypeError):
                         logger.warning(f"Could not parse client ID from selected row {selected_row}, column 0.")
            logger.debug(f"Previously selected client ID: {selected_client_id}")
            # ----------------------

            all_clients = self.server.get_all_clients()
            self.all_clients_table.setRowCount(0) # Clear table
            self.all_clients_table.setRowCount(len(all_clients))
            
            row_to_reselect = -1 # Default to no reselection
            
            for row, client in enumerate(all_clients):
                # Assuming columns: ID, Username, Registered At, Last Seen, Total Queries
                client_id = client['id']
                item_id = QTableWidgetItem(str(client_id))
                item_id.setData(Qt.UserRole, client_id) # Store ID for easy access
                item_username = QTableWidgetItem(client['nickname'])
                item_registered = QTableWidgetItem(format_timestamp(client['registration_date']))
                item_last_seen = QTableWidgetItem(format_timestamp(client['last_seen'], default="Never"))
                item_total_queries = QTableWidgetItem(str(client['total_queries'] or 0))

                self.all_clients_table.setItem(row, 0, item_id)
                self.all_clients_table.setItem(row, 1, item_username)
                self.all_clients_table.setItem(row, 2, item_registered)
                self.all_clients_table.setItem(row, 3, item_last_seen)
                self.all_clients_table.setItem(row, 4, item_total_queries)
                
                # Check if this row matches the previously selected client
                if selected_client_id is not None and client_id == selected_client_id:
                     row_to_reselect = row
                     logger.debug(f"Found row {row} to reselect for client ID {client_id}")
            
            # Resize columns to fit content
            self.all_clients_table.resizeColumnsToContents()
            
            # --- Reselect previously selected row WITHOUT triggering handler --- 
            if row_to_reselect != -1:
                logger.debug(f"Reselecting row {row_to_reselect} for client ID {selected_client_id}")
                # Temporarily block signals from the table to prevent on_all_client_selected from firing
                self.all_clients_table.blockSignals(True)
                self.all_clients_table.selectRow(row_to_reselect)
                self.all_clients_table.blockSignals(False)
            else:
                self.client_details_area.clear()
            
            logger.info(f"'All Clients' table updated with {len(all_clients)} clients.")
        except AttributeError as ae:
             # This can happen if self.server is None (e.g., not started)
             logger.warning(f"Could not update 'All Clients': Server object not available? {ae}")
        except RuntimeError as e:
            logger.error(f"Runtime error updating 'All Clients': {e}. Database pool might be closed.", exc_info=False) # Don't need full trace usually
        except Exception as e:
            logger.error(f"Error updating 'All Clients' table: {e}", exc_info=True)

    def on_client_selected(self, item=None):
        """Handles the selection of a client in the active clients tree.
        Updates the UI to enable sending direct messages and sets the placeholder text.
        """
        self.selected_active_client_id = None # Clear previous selection
        selected_items = self.active_clients_tree.selectedItems()
        if selected_items:
            self.send_client_button.setEnabled(True)
            
            selected_item = selected_items[0]
            
            # Retrieve the actual client ID stored in the item data
            self.selected_active_client_id = selected_item.data(0, Qt.UserRole)

            # Update placeholder text (use info from columns)
            client_id_text = selected_item.text(0) # Column 0: Client ID
            client_address_text = selected_item.text(1) # Column 1: IP Address
            
            if self.selected_active_client_id is not None:
                self.client_message_input.setPlaceholderText(f"Send message to Client {self.selected_active_client_id} ({client_address_text})")
            else:
                 # Fallback if ID is somehow missing
                 self.client_message_input.setPlaceholderText(f"Send message to {client_address_text})")
        else:
            self.send_client_button.setEnabled(False)
            self.client_message_input.setPlaceholderText("Select a client first")
    
    def on_all_client_selected(self):
        """Handles selection change in the 'All Clients' table.
        Fetches and displays detailed information (basic info, query stats, session history)
        for the selected client in the details view area.
        """
        selected_items = self.all_clients_table.selectedItems()
        if selected_items:
            selected_item = selected_items[0]
            client_id = selected_item.data(Qt.UserRole) # Retrieve stored client ID

            # Only update details if the selected client ID has changed
            if client_id == self.currently_displayed_client_id:
                # logger.debug(f"Client {client_id} already displayed, skipping detail update.")
                return # Skip update if same client is re-selected

            if client_id is not None:
                # Correctly get the username from column 1 of the selected row
                selected_row = selected_item.row()
                username_item = self.all_clients_table.item(selected_row, 1) # Column 1 for Username
                username = username_item.text() if username_item else "Unknown" # Get text if item exists
                logger.info(f"Client selected in 'All Clients': ID={client_id}, Username={username}")

                # Fetch details from the server
                details_text = ""
                if self.server and self.server_running: # Check server is running
                    try:
                        client_details = self.server.get_client_details(client_id)
                        if client_details:
                            details_text = self.format_client_details(client_details)
                        else:
                            details_text = f"Could not retrieve details for Client ID: {client_id}."
                    except Exception as e:
                        logger.error(f"Error fetching client details for ID {client_id}: {e}", exc_info=True)
                        details_text = f"Error fetching details for Client ID: {client_id}\n{e}"
                else:
                    details_text = "Server not running. Cannot fetch client details."

                # Update the text area and store the newly displayed ID
                self.client_details_area.setText(details_text)
                self.currently_displayed_client_id = client_id
            else:
                logger.warning("Selected item in 'All Clients' table has no client ID.")
                self.client_details_area.setText("Error: Could not retrieve client ID for selected item.")
                self.currently_displayed_client_id = None # Clear tracked ID
        else:
            # No item selected, clear the details area
            self.client_details_area.clear()
            self.currently_displayed_client_id = None # Clear tracked ID

    def format_client_details(self, details):
        """Helper function to format a client's detailed information dictionary into a displayable string."""
        if not details:
            return "No details available."

        text = f"--- Client Details ---\n"
        text += f"ID: {details.get('id', 'N/A')}\n"
        text += f"Name: {details.get('name', 'N/A')}\n"
        text += f"Nickname: {details.get('nickname', 'N/A')}\n"
        text += f"Email: {details.get('email', 'N/A')}\n"
        text += f"Registered: {format_timestamp(details.get('registration_date'))}\n"
        text += f"Last Login: {format_timestamp(details.get('last_login'), default='Never')}\n"
        text += f"\n--- Query Statistics ---\n"
        query_stats = details.get('query_stats', [])
        if query_stats:
            # Sort stats by query_type (e.g., 'query1', 'query2')
            query_stats.sort(key=lambda x: x.get('query_type', ''))
            for stat in query_stats:
                text += f"- {stat.get('query_type', 'Unknown')}: {stat.get('count', 0)} times\n"
        else:
            text += "No query statistics available.\n"

        text += f"\n--- Session History (Last 5) ---\n"
        session_history = details.get('session_history', [])
        if session_history:
            for session in session_history[:5]: # 5 is genoeg I guess?
                start = format_timestamp(session.get('start_time'))
                end = format_timestamp(session.get('end_time'), default='Active')
                duration_s = session.get('duration_seconds')
                duration_str = f"{duration_s:.1f}s" if duration_s is not None else "N/A"
                text += f"- Session {session.get('id')}: {start} - {end} ({duration_str}) from {session.get('address', 'Unknown')}\n"
        else:
            text += "No session history available.\n"

        return text
    
    def schedule_updates(self):
        """Periodically called by a QTimer to update dynamic GUI components like
        the active client list and server statistics.
        """
        
        if self.server_running and self.server:
            self.update_active_clients()
            self.update_statistics()
        elif self.server is None:
            # If server object is None, ensure UI reflects stopped state (e.g., clear lists)
            self.active_clients_tree.clear()
            self.all_clients_table.clear()
            self.update_statistics()
    
    def closeEvent(self, event):
        """Handles the window close event. Prompts the user to stop the server
        if it is currently running before closing the application.
        """
        if self.server_running:
            reply = QMessageBox.question(
                self, "Quit", 
                "Server is still running. Do you want to stop it and quit?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                self.server.stop()
                event.accept()
            else:
                event.ignore()
        else:
            event.accept()

    def update_statistics(self):
        """Updates the statistics labels (uptime, client counts, query counts)
        and refreshes the daily query trends plot. This method is safe to call
        even when the server is None or not running, in which case it displays a stopped state.
        """
        if not self.server_running or not self.server:
            # Server stopped or not initialized - Set labels to stopped state
            self.uptime_label.setText("Stopped")
            self.registered_clients_label.setText("N/A") # Changed to N/A as we can't query DB
            self.total_queries_label.setText("N/A")
            # TODO: Reset other specific query stats labels here if they exist
            logger.debug("update_statistics: Server stopped, resetting labels.")
            self.stats_canvas.figure.clf()
            ax = self.stats_canvas.figure.subplots()
            ax.text(0.5, 0.5, "Server Stopped", ha='center', va='center')
            ax.set_xticks([])
            ax.set_yticks([])
            self.stats_canvas.draw()
            return

        # Calculate Uptime
        try:
            start_time = getattr(self.server, 'start_time', None)
            if start_time:
                uptime_seconds = time.time() - start_time
                days, rem = divmod(uptime_seconds, 86400)
                hours, rem = divmod(rem, 3600)
                minutes, seconds = divmod(rem, 60)
                uptime_str = ""
                if days > 0: uptime_str += f"{int(days)}d "
                if hours > 0 or days > 0: uptime_str += f"{int(hours)}h "
                if minutes > 0 or hours > 0 or days > 0: uptime_str += f"{int(minutes)}m "
                uptime_str += f"{int(seconds)}s"
                self.uptime_label.setText(uptime_str.strip())
            else:
                self.uptime_label.setText("Calculating...")
        except Exception as e:
            logger.warning("Could not calculate uptime.")
            self.uptime_label.setText("Error") # Indicate error calculating uptime

        # Get Total Registered Clients from db
        try:
            all_clients = self.server.get_all_clients() # This method accesses DB
            self.registered_clients_label.setText(str(len(all_clients)))
        except RuntimeError as rterr:
            if "pool is closed" in str(rterr):
                logger.warning(f"DB Pool closed while getting all clients for stats: {rterr}")
                self.registered_clients_label.setText("N/A")
            else:
                 logger.error(f"RuntimeError getting all clients for stats: {rterr}")
                 self.registered_clients_label.setText("Error")
        except Exception as e:
            logger.error(f"Error getting all clients for stats: {e}")
            self.registered_clients_label.setText("Error")

        # Get Total Queries Processed (Needs server method accessing DB)
        try:
            query_stats = self.server.get_query_stats() # This method accesses DB
            total_queries = sum(stat.get("count", 0) for stat in query_stats)
            self.total_queries_label.setText(str(total_queries))
            # TODO: Update specific query type labels if needed
        except RuntimeError as rterr:
            if "pool is closed" in str(rterr):
                logger.warning(f"DB Pool closed while getting query stats: {rterr}")
                self.total_queries_label.setText("N/A")
            else:
                 logger.error(f"RuntimeError getting query stats: {rterr}")
                 self.total_queries_label.setText("Error")
        except Exception as e:
            logger.error(f"Error getting query stats: {e}")
            self.total_queries_label.setText("Error")

        # Log completion
        logger.debug("update_statistics: Updated labels for running server.")

        # --- Update Daily Query Trends Plot --- 
        try:
            daily_counts_data = self.server.get_daily_query_counts()
            if daily_counts_data:
                # Process data with pandas
                df_daily = pd.DataFrame(daily_counts_data)
                df_daily['query_date'] = pd.to_datetime(df_daily['query_date'])
                # Pivot table: dates as index, query types as columns, count as values
                pivot_df = df_daily.pivot_table(index='query_date', columns='query_type', values='count', fill_value=0)
                # Ensure all expected query types are present as columns, even if count is 0
                all_query_types = [f'query{i}' for i in range(1, 5)] # Assuming query1-query4
                for q_type in all_query_types:
                    if q_type not in pivot_df.columns:
                        pivot_df[q_type] = 0
                pivot_df = pivot_df[all_query_types] # Ensure consistent column order

                # --- Ensure index is just date (not datetime) --- 
                pivot_df.index = pivot_df.index.date
                # -----------------------------------------------

                # --- Plotting --- 
                # Clear the previous figure/axes
                self.stats_canvas.figure.clf()
                ax = self.stats_canvas.figure.subplots()

                # Plot each query type as a line
                pivot_df.plot(kind='line', marker='.', ax=ax)

                ax.set_title("Daily Query Usage Trends")
                ax.set_xlabel("Date")
                ax.set_ylabel("Number of Queries")
                ax.legend(title="Query Type")
                ax.grid(True, linestyle='--', alpha=0.6)
                
                # --- Explicitly format x-axis date labels and set locator --- 
                date_format = mdates.DateFormatter('%Y-%m-%d') # Format as YYYY-MM-DD
                day_locator = mdates.DayLocator() # Locate ticks on days
                ax.xaxis.set_major_locator(day_locator)
                ax.xaxis.set_major_formatter(date_format)
                # ----------------------------------------------------------
                
                self.stats_canvas.figure.autofmt_xdate() # Improve date label formatting
                self.stats_canvas.draw() # Redraw the canvas
                logger.debug("Updated daily query trends plot.")
            else:
                # No daily data, clear the plot
                self.stats_canvas.figure.clf()
                ax = self.stats_canvas.figure.subplots()
                ax.text(0.5, 0.5, "No daily query data available", ha='center', va='center')
                ax.set_xticks([])
                ax.set_yticks([])
                self.stats_canvas.draw()
                logger.debug("Cleared daily query trends plot (no data).")

        except Exception as plot_err:
            logger.error(f"Error updating daily query trends plot: {plot_err}", exc_info=True)
            # Optionally display error on the plot canvas
            try:
                self.stats_canvas.figure.clf()
                ax = self.stats_canvas.figure.subplots()
                ax.text(0.5, 0.5, f"Error plotting trends:\n{plot_err}", ha='center', va='center', color='red')
                ax.set_xticks([])
                ax.set_yticks([])
                self.stats_canvas.draw()
            except Exception as display_err:
                 logger.error(f"Failed to display plot error message: {display_err}")

    def update_dynamic_query_stats_labels(self, query_stats):
        # Implementation of update_dynamic_query_stats_labels method
        pass

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    settings = QSettings("ArrestDataApp", "Server/AppSettings")
    dark_theme = settings.value("dark_theme", True, type=bool)
    
    if dark_theme:
        app.setStyleSheet(DARK_STYLESHEET)
    else:
        app.setStyleSheet(LIGHT_STYLESHEET)
    
    window = ServerGUI()
    window.show()
    sys.exit(app.exec()) 