#!/usr/bin/env python3
# GUI for the server application (using PySide6)

import os
import sys
import threading
import time
import datetime

# Add the parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import server module
from .server import Server

# Import PySide6 modules
from PySide6.QtWidgets import (
    QMainWindow, QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QLineEdit, QTextEdit, QTabWidget, QTreeWidget, QTreeWidgetItem,
    QGroupBox, QMessageBox, QSplitter, QStatusBar, QFormLayout, QFrame, QCheckBox
)
from PySide6.QtCore import Qt, QTimer, Signal, Slot, QDateTime, QSettings
from PySide6.QtGui import QFont, QPalette, QColor

# Dark theme stylesheet
DARK_STYLESHEET = """
QWidget {
    background-color: #2D2D30;
    color: #E1E1E1;
}

QMainWindow, QDialog {
    background-color: #1E1E1E;
}

QTabWidget::pane {
    border: 1px solid #3F3F46;
    background-color: #2D2D30;
}

QTabBar::tab {
    background-color: #3F3F46;
    color: #E1E1E1;
    padding: 6px 12px;
    border: 1px solid #3F3F46;
    border-bottom: none;
    border-top-left-radius: 4px;
    border-top-right-radius: 4px;
}

QTabBar::tab:selected {
    background-color: #007ACC;
}

QGroupBox {
    border: 1px solid #3F3F46;
    border-radius: 4px;
    margin-top: 0.5em;
    padding-top: 0.5em;
}

QGroupBox::title {
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 3px;
}

QPushButton {
    background-color: #3F3F46;
    color: #E1E1E1;
    border: 1px solid #3F3F46;
    padding: 4px 8px;
    border-radius: 4px;
}

QPushButton:hover {
    background-color: #505050;
}

QPushButton:pressed {
    background-color: #007ACC;
}

QLineEdit, QTextEdit, QComboBox, QSpinBox {
    background-color: #1E1E1E;
    color: #E1E1E1;
    border: 1px solid #3F3F46;
    padding: 2px;
    border-radius: 2px;
}

QTreeWidget {
    background-color: #1E1E1E;
    alternate-background-color: #2D2D30;
    color: #E1E1E1;
    border: 1px solid #3F3F46;
}

QTreeWidget::item:selected {
    background-color: #007ACC;
}

QHeaderView::section {
    background-color: #3F3F46;
    color: #E1E1E1;
    padding: 4px;
    border: 1px solid #3F3F46;
}

QScrollBar:vertical {
    background-color: #2D2D30;
    width: 12px;
    margin: 0px;
}

QScrollBar::handle:vertical {
    background-color: #3F3F46;
    min-height: 20px;
    border-radius: 6px;
}

QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0px;
}

QScrollBar:horizontal {
    background-color: #2D2D30;
    height: 12px;
    margin: 0px;
}

QScrollBar::handle:horizontal {
    background-color: #3F3F46;
    min-width: 20px;
    border-radius: 6px;
}

QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
    width: 0px;
}

QStatusBar {
    background-color: #007ACC;
    color: white;
}

QSplitter::handle {
    background-color: #3F3F46;
}

QFrame[frameShape="4"], QFrame[frameShape="5"] {
    background-color: #3F3F46;
}
"""

# Light theme stylesheet
LIGHT_STYLESHEET = """
QWidget {
    background-color: #F0F0F0;
    color: #202020;
}

QTabBar::tab:selected {
    background-color: #007ACC;
    color: white;
}

QPushButton {
    background-color: #E0E0E0;
    border: 1px solid #C0C0C0;
    padding: 4px 8px;
    border-radius: 4px;
}

QPushButton:hover {
    background-color: #D0D0D0;
}

QPushButton:pressed {
    background-color: #007ACC;
    color: white;
}

QGroupBox {
    border: 1px solid #C0C0C0;
    border-radius: 4px;
    margin-top: 0.5em;
    padding-top: 0.5em;
}

QTreeWidget::item:selected {
    background-color: #007ACC;
    color: white;
}

QStatusBar {
    background-color: #007ACC;
    color: white;
}
"""

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
        return str(ts_str) # Return original string if parsing fails

class ServerGUI(QMainWindow):
    """GUI for the server application using PySide6"""
    
    def __init__(self):
        """Initialize the GUI"""
        super().__init__()
        
        # Set window properties
        self.setWindowTitle("Arrest Data Server")
        self.resize(900, 700)
        self.setMinimumSize(800, 600)
        
        # Initialize settings
        self.settings = QSettings("ArrestDataApp", "Server/AppSettings")
        
        # Default theme to dark
        self.dark_theme = self.settings.value("dark_theme", True, type=bool)
        
        # Server instance
        self.server = Server()
        
        # Register callbacks
        self.server.on_activity_log = self.on_activity_log
        self.server.on_client_list_update = self.update_client_list
        
        # Setup GUI components
        self.setup_gui()
        
        # Apply theme
        self.apply_theme()
        
        # Server status
        self.server_running = False
        
        # Start update timer
        self.update_timer = QTimer(self)
        self.update_timer.timeout.connect(self.schedule_updates)
        self.update_timer.start(5000)  # Update every 5 seconds
    
    def setup_gui(self):
        """Setup GUI components"""
        # Create central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Main layout
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(5, 5, 5, 5)  # Reduce margins
        main_layout.setSpacing(5)  # Reduce spacing
        
        # Theme toggle in header
        header_layout = QHBoxLayout()
        header_layout.setSpacing(5)  # Reduce spacing
        
        # Add spacer to push theme toggle to the right
        header_layout.addStretch(1)
        
        # Theme toggle checkbox
        self.theme_toggle = QCheckBox("Dark Theme")
        self.theme_toggle.setChecked(self.dark_theme)
        self.theme_toggle.stateChanged.connect(self.toggle_theme)
        header_layout.addWidget(self.theme_toggle)
        
        # Add header to main layout
        main_layout.addLayout(header_layout)
        
        # Create notebook for tabs
        self.tab_widget = QTabWidget()
        main_layout.addWidget(self.tab_widget)
        
        # Create tabs
        self.setup_server_tab()
        self.setup_clients_tab()
        self.setup_stats_tab()
        
        # Create status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Server not running")
    
    def apply_theme(self):
        """Apply the current theme to the application"""
        if self.dark_theme:
            QApplication.instance().setStyleSheet(DARK_STYLESHEET)
        else:
            QApplication.instance().setStyleSheet(LIGHT_STYLESHEET)
    
    def toggle_theme(self):
        """Toggle between light and dark themes"""
        self.dark_theme = self.theme_toggle.isChecked()
        # Save the setting
        self.settings.setValue("dark_theme", self.dark_theme)
        # Apply the theme
        self.apply_theme()
    
    def setup_server_tab(self):
        """Setup server control tab"""
        # Create server tab
        server_tab = QWidget()
        self.tab_widget.addTab(server_tab, "Server Control")
        
        # Server tab layout
        server_layout = QVBoxLayout(server_tab)
        server_layout.setContentsMargins(5, 5, 5, 5)  # Reduce margins
        server_layout.setSpacing(5)  # Reduce spacing
        
        # Create server control group
        control_group = QGroupBox("Server Control")
        control_layout = QHBoxLayout(control_group)
        control_layout.setContentsMargins(5, 10, 5, 5)  # Reduce margins
        control_layout.setSpacing(5)  # Reduce spacing
        
        # Create server control buttons
        self.start_button = QPushButton("Start Server")
        self.start_button.clicked.connect(self.start_server)
        control_layout.addWidget(self.start_button)
        
        self.stop_button = QPushButton("Stop Server")
        self.stop_button.clicked.connect(self.stop_server)
        self.stop_button.setEnabled(False)
        control_layout.addWidget(self.stop_button)
        
        # Add stretch to push buttons to the left
        control_layout.addStretch()
        
        # Add control group to server layout
        server_layout.addWidget(control_group)
        
        # Create message group
        message_group = QGroupBox("Broadcast Message")
        message_layout = QHBoxLayout(message_group)
        message_layout.setContentsMargins(5, 10, 5, 5)  # Reduce margins
        message_layout.setSpacing(5)  # Reduce spacing
        
        # Create message entry
        self.message_entry = QLineEdit()
        message_layout.addWidget(self.message_entry)
        
        # Create send button
        self.send_button = QPushButton("Broadcast")
        self.send_button.clicked.connect(self.broadcast_message)
        self.send_button.setEnabled(False)
        message_layout.addWidget(self.send_button)
        
        # Add message group to server layout
        server_layout.addWidget(message_group)
        
        # Create server log group
        log_group = QGroupBox("Server Activity Log")
        log_layout = QVBoxLayout(log_group)
        log_layout.setContentsMargins(5, 10, 5, 5)  # Reduce margins
        log_layout.setSpacing(5)  # Reduce spacing
        
        # Create server log text area
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        log_layout.addWidget(self.log_text)
        
        # Add log group to server layout
        server_layout.addWidget(log_group)
        
        # Make log group expand to fill available space
        server_layout.setStretchFactor(log_group, 1)
    
    def setup_clients_tab(self):
        """Setup clients tab"""
        # Create clients tab
        clients_tab = QWidget()
        self.tab_widget.addTab(clients_tab, "Clients")
        
        # Clients tab layout
        clients_layout = QVBoxLayout(clients_tab)
        clients_layout.setContentsMargins(5, 5, 5, 5)  # Reduce margins
        clients_layout.setSpacing(5)  # Reduce spacing
        
        # Create splitter for active clients and all clients
        splitter = QSplitter(Qt.Vertical)
        
        # Create active clients group
        active_clients_group = QGroupBox("Active Clients")
        active_clients_layout = QVBoxLayout(active_clients_group)
        active_clients_layout.setContentsMargins(5, 10, 5, 5)  # Reduce margins
        active_clients_layout.setSpacing(5)  # Reduce spacing
        
        # Create active clients tree
        self.active_clients_tree = QTreeWidget()
        self.active_clients_tree.setHeaderLabels(["Client", "Name", "Nickname", "Email", "Connected Since", "Last Activity"])
        self.active_clients_tree.setAlternatingRowColors(True)  # Better readability
        self.active_clients_tree.setRootIsDecorated(False)
        self.active_clients_tree.setWordWrap(True)
        self.active_clients_tree.setColumnWidth(0, 150)
        self.active_clients_tree.setColumnWidth(1, 150)
        self.active_clients_tree.setColumnWidth(2, 150)
        self.active_clients_tree.setColumnWidth(3, 200)
        self.active_clients_tree.setColumnWidth(4, 150)
        self.active_clients_tree.setSelectionMode(QTreeWidget.SingleSelection)
        self.active_clients_tree.itemSelectionChanged.connect(self.on_client_selected)
        active_clients_layout.addWidget(self.active_clients_tree)
        
        # Create client message group
        client_message_group = QGroupBox("Send Message to Selected Client")
        client_message_layout = QHBoxLayout(client_message_group)
        client_message_layout.setContentsMargins(5, 10, 5, 5)  # Reduce margins
        client_message_layout.setSpacing(5)  # Reduce spacing
        
        # Create client message entry
        self.client_message_entry = QLineEdit()
        client_message_layout.addWidget(self.client_message_entry)
        
        # Create client send button
        self.client_send_button = QPushButton("Send")
        self.client_send_button.clicked.connect(self.send_client_message)
        self.client_send_button.setEnabled(False)
        client_message_layout.addWidget(self.client_send_button)
        
        # Add client message group to active clients layout
        active_clients_layout.addWidget(client_message_group)
        
        # Create all clients group
        all_clients_group = QGroupBox("All Clients")
        all_clients_layout = QVBoxLayout(all_clients_group)
        all_clients_layout.setContentsMargins(5, 10, 5, 5)  # Reduce margins
        all_clients_layout.setSpacing(5)  # Reduce spacing
        
        # Create all clients tree
        self.all_clients_tree = QTreeWidget()
        self.all_clients_tree.setHeaderLabels(["Email", "Name", "Nickname", "Last Login", "Registration Date"])
        self.all_clients_tree.setAlternatingRowColors(True)  # Better readability
        self.all_clients_tree.setRootIsDecorated(False)
        self.all_clients_tree.setColumnWidth(0, 200)
        self.all_clients_tree.setColumnWidth(1, 150)
        self.all_clients_tree.setColumnWidth(2, 150)
        self.all_clients_tree.setColumnWidth(3, 150)
        self.all_clients_tree.setSelectionMode(QTreeWidget.SingleSelection)
        self.all_clients_tree.itemSelectionChanged.connect(self.on_all_client_selected)
        all_clients_layout.addWidget(self.all_clients_tree)
        
        # Add groups to splitter
        splitter.addWidget(active_clients_group)
        splitter.addWidget(all_clients_group)
        
        # Set initial sizes
        splitter.setSizes([400, 400])
        
        # Add splitter to clients layout
        clients_layout.addWidget(splitter)
    
    def setup_stats_tab(self):
        """Setup stats tab"""
        # Create stats tab
        stats_tab = QWidget()
        self.tab_widget.addTab(stats_tab, "Statistics")
        
        # Stats tab layout
        stats_layout = QVBoxLayout(stats_tab)
        stats_layout.setContentsMargins(5, 5, 5, 5)  # Reduce margins
        stats_layout.setSpacing(5)  # Reduce spacing
        
        # Create server stats group
        stats_group = QGroupBox("Server Statistics")
        stats_form_layout = QFormLayout(stats_group)
        stats_form_layout.setContentsMargins(5, 10, 5, 5)  # Reduce margins
        stats_form_layout.setSpacing(5)  # Reduce spacing
        
        # Create stats labels
        self.uptime_label = QLabel("Not started")
        stats_form_layout.addRow("Server Uptime:", self.uptime_label)
        
        self.connections_label = QLabel("0")
        stats_form_layout.addRow("Total Connections:", self.connections_label)
        
        self.active_clients_label = QLabel("0")
        stats_form_layout.addRow("Active Clients:", self.active_clients_label)
        
        self.total_clients_label = QLabel("0")
        stats_form_layout.addRow("Registered Clients:", self.total_clients_label)
        
        self.queries_label = QLabel("0")
        stats_form_layout.addRow("Queries Processed:", self.queries_label)
        
        self.errors_label = QLabel("0")
        stats_form_layout.addRow("Errors:", self.errors_label)
        
        # Add stats group to stats layout
        stats_layout.addWidget(stats_group)
        
        # Add stretch to push stats to the top
        stats_layout.addStretch()
    
    def start_server(self):
        """Start the server"""
        if not self.server_running:
            if self.server.start():
                self.server_running = True
                self.status_bar.showMessage(f"Server running on {self.server.host}:{self.server.port}")
                self.start_button.setEnabled(False)
                self.stop_button.setEnabled(True)
                self.send_button.setEnabled(True)
                QMessageBox.information(self, "Server", "Server started successfully!")
            else:
                QMessageBox.critical(self, "Server", "Failed to start server!")
    
    def stop_server(self):
        """Stop the server"""
        if self.server_running:
            reply = QMessageBox.question(
                self, "Stop Server", 
                "Are you sure you want to stop the server?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                self.server.stop()
                self.server_running = False
                self.status_bar.showMessage("Server not running")
                self.start_button.setEnabled(True)
                self.stop_button.setEnabled(False)
                self.send_button.setEnabled(False)
                self.client_send_button.setEnabled(False)
    
    def broadcast_message(self):
        """Broadcast a message to all clients"""
        message = self.message_entry.text().strip()
        if message:
            if self.server.broadcast_message(message):
                self.message_entry.clear()
                QMessageBox.information(self, "Message", "Message broadcast to all clients!")
            else:
                QMessageBox.warning(self, "Message", "No clients connected to broadcast to!")
        else:
            QMessageBox.warning(self, "Message", "Please enter a message to broadcast!")
    
    def send_client_message(self):
        """Send message to selected client"""
        # Get selected client
        selected_items = self.active_clients_tree.selectedItems()
        if not selected_items:
            return
        
        # Get message text
        message_text = self.client_message_entry.text()
        if not message_text:
            # Show error message if message is empty
            QMessageBox.warning(self, "Error", "Message cannot be empty")
            return
        
        # Get the selected client
        selected_item = selected_items[0]
        
        # First, find the client ID by address in the active clients list
        active_clients = self.server.get_active_clients()
        client_id = None
        client_address = selected_item.text(0)
        client_email = selected_item.text(3)
        
        for client in active_clients:
            if client.get("address") == client_address:
                client_id = client.get("id")
                break
        
        if client_id is None:
            QMessageBox.warning(self, "Error", "Client not found or not connected")
            return
        
        # Send message to client using the correct server method
        success = self.server.send_message_to_client(client_id, message_text)
        
        if success:
            # Clear message entry
            self.client_message_entry.clear()
            
            # Show success message
            self.status_bar.showMessage(f"Message sent to {client_email} ({client_address})", 3000)
        else:
            # Show error message
            QMessageBox.warning(self, "Error", "Failed to send message to client")
    
    def on_activity_log(self, timestamp, message):
        """Handle server activity log updates"""
        # Format the timestamp
        try:
            dt = datetime.datetime.fromisoformat(timestamp)
            formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            formatted_time = timestamp
        
        log_entry = f"[{formatted_time}] {message}"
        
        # Update the log text
        self.log_text.append(log_entry)
        # Scroll to the bottom
        scrollbar = self.log_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
    
    def update_client_list(self):
        """Update the client list"""
        # Update active clients
        self.update_active_clients()
    
    def update_active_clients(self):
        """Update the active clients tree"""
        self.active_clients_tree.clear()
        active_clients = self.server.get_active_clients()
        for client in active_clients:
            item = QTreeWidgetItem([
                client.get("address", "Unknown"),
                client.get("name", "Unknown"),
                client.get("nickname", "Unknown"),
                client.get("email", "Unknown"),
                format_timestamp(client.get("connected_since")),
                "N/A"
            ])
            self.active_clients_tree.addTopLevelItem(item)
        self.active_clients_label.setText(str(len(active_clients)))
    
    def update_all_clients(self):
        """Update the all clients tree"""
        self.all_clients_tree.clear()
        all_clients = self.server.get_all_clients()
        for client in all_clients:
            item = QTreeWidgetItem([
                client.get("email", "Unknown"),
                client.get("name", "Unknown"),
                client.get("nickname", "Unknown"),
                format_timestamp(client.get("last_login"), default="Never"),
                format_timestamp(client.get("registration_date"))
            ])
            self.all_clients_tree.addTopLevelItem(item)
        self.total_clients_label.setText(str(len(all_clients)))
        
        # Update active clients count
        active_clients = self.server.get_active_clients()
        self.active_clients_label.setText(str(len(active_clients)))
        
        # Calculate uptime if server is running
        if self.server_running:
            uptime_seconds = time.time() - self.server.start_time if hasattr(self.server, 'start_time') else 0
            days, remainder = divmod(uptime_seconds, 86400)
            hours, remainder = divmod(remainder, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            if days > 0:
                uptime_str = f"{int(days)}d {int(hours)}h {int(minutes)}m {int(seconds)}s"
            elif hours > 0:
                uptime_str = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
            else:
                uptime_str = f"{int(minutes)}m {int(seconds)}s"
                
            self.uptime_label.setText(uptime_str)
        else:
            self.uptime_label.setText("Not started")
        
        # Set default values for other stats that we don't have access to
        # You can add real implementations if these stats become available
        self.connections_label.setText(str(len(active_clients)))  # Using active clients as a proxy
        
        # Get query stats if available
        try:
            query_stats = self.server.get_query_stats()
            total_queries = sum(stat.get("count", 0) for stat in query_stats)
            self.queries_label.setText(str(total_queries))
        except (AttributeError, Exception):
            self.queries_label.setText("N/A")
            
        # We don't have error stats, so set to N/A
        self.errors_label.setText("N/A")
    
    def on_client_selected(self, item=None):
        """Handle client selection in the active clients tree"""
        # Enable send button if client is selected
        selected_items = self.active_clients_tree.selectedItems()
        if selected_items:
            self.client_send_button.setEnabled(True)
            
            # Get the selected client
            selected_item = selected_items[0]
            client_address = selected_item.text(0)
            client_email = selected_item.text(3)
            
            # Show client address in message entry placeholder
            self.client_message_entry.setPlaceholderText(f"Send message to {client_email} ({client_address})")
        else:
            self.client_send_button.setEnabled(False)
            self.client_message_entry.setPlaceholderText("Select a client first")
    
    def on_all_client_selected(self, item=None):
        """Handle client selection in the all clients tree"""
        # Currently no action needed
        pass
    
    def schedule_updates(self):
        """Schedule updates for dynamic components"""
        # Only update if the server is running
        if self.server_running:
            self.update_active_clients()
            self.update_all_clients()
    
    def closeEvent(self, event):
        """Handle window close event"""
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


if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # Load settings to check for initial theme
    settings = QSettings("ArrestDataApp", "Server/AppSettings")
    dark_theme = settings.value("dark_theme", True, type=bool)
    
    # Apply initial theme before creating the UI
    if dark_theme:
        app.setStyleSheet(DARK_STYLESHEET)
    else:
        app.setStyleSheet(LIGHT_STYLESHEET)
    
    window = ServerGUI()
    window.show()
    sys.exit(app.exec()) 