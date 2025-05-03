#!/usr/bin/env python3
# GUI Client for the arrest data client-server application

import os
import sys
import time
import logging
from datetime import datetime
import re 
import webbrowser 
import tempfile
from pathlib import Path # <--- ADD THIS IMPORT

# Add the parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import client module
from client.client import Client
from client.gui_stylingsheets import DARK_STYLESHEET, LIGHT_STYLESHEET

# Import shared modules
from shared.constants import *

# Import PySide6 modules
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QTabWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QTextEdit, QComboBox, QSpinBox, QGroupBox,
    QFormLayout, QSplitter, QTableWidget, QTableWidgetItem, QMessageBox,
    QStatusBar, QScrollArea, QGridLayout, QDialog, QFileDialog, QCheckBox,
    QDateEdit, QDoubleSpinBox, QStackedWidget, QListWidget, QListWidgetItem
)
from PySide6.QtGui import QPixmap, QFont, QIcon, QPalette, QColor
from PySide6.QtCore import Qt, QTimer, Signal, Slot, QObject, QSettings, QDate, QEvent

# Import necessary for rendering Matplotlib figure in Qt
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg

from client.gui_widgets import *

# --- Configure GUI logging to file ---
TEMP_DIR_GUI = tempfile.gettempdir()
CLIENT_GUI_LOG_FILE = os.path.join(TEMP_DIR_GUI, 'client_gui_temp.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=CLIENT_GUI_LOG_FILE, # <-- Log to file
    filemode='w'                  # <-- Overwrite file each time
    # handlers=[ ... ]          # <-- Remove console handler
)
logger = logging.getLogger('client_gui')



# Bridge class to convert client callbacks to Qt signals
class ClientCallbacksBridge(QObject):
    """Bridge to convert client callbacks to Qt signals"""
    
    # Define signals
    connection_status_changed = Signal(bool)
    login_status_changed = Signal(bool)
    message_received = Signal(str, str)
    query_result_received = Signal(object)
    error_occurred = Signal(str)
    
    def __init__(self):
        super().__init__()
    
    def on_connection_status_change(self, connected):
        """Called when connection status changes"""
        self.connection_status_changed.emit(connected)
    
    def on_login_status_change(self, logged_in):
        """Called when login status changes"""
        self.login_status_changed.emit(logged_in)
    
    def on_message_received(self, timestamp, message):
        """Called when message is received from server"""
        self.message_received.emit(timestamp, message)
    
    def on_query_result(self, result):
        """Called when query result is received"""
        self.query_result_received.emit(result)
    
    def on_error(self, error_message):
        """Called when error occurs"""
        self.error_occurred.emit(error_message)


class PlotViewerDialog(QDialog):
    """Dialog for viewing Matplotlib figures in a larger size with save option."""
    
    def __init__(self, fig, title="Plot Viewer", parent=None):
        super().__init__(parent)
        self.figure = fig # Store the figure object
        self.setWindowTitle(title)
        self.setModal(True)
        
        # Set a large initial size (e.g., 80% of screen)
        screen_size = QApplication.primaryScreen().size()
        self.resize(int(screen_size.width() * 0.8), int(screen_size.height() * 0.8))
        
        # Main layout
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)
        
        # Create FigureCanvas
        self.plot_canvas = FigureCanvasQTAgg(self.figure)
        
        # Add canvas to layout (allow stretching)
        layout.addWidget(self.plot_canvas, 1)
        
        # Create buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        self.save_button = QPushButton(QIcon.fromTheme("document-save"), "Save Image")
        self.save_button.clicked.connect(self.save_image)
        button_layout.addWidget(self.save_button)
        
        self.close_button = QPushButton(QIcon.fromTheme("window-close"), "Close")
        self.close_button.clicked.connect(self.accept)
        button_layout.addWidget(self.close_button)
        
        layout.addLayout(button_layout)
    
    def save_image(self):
        """Save the plot image to a file using the figure's savefig."""
        filename, selected_filter = QFileDialog.getSaveFileName(
            self, 
            "Save Plot Image", 
            "", # Default directory
            "PNG Image (*.png);;JPEG Image (*.jpg *.jpeg);;PDF Document (*.pdf);;All Files (*)"
        )
        if filename:
            try:
                # Use the stored figure object's savefig method
                self.figure.savefig(filename) # Let savefig determine format from extension
                # Show confirmation in main window status bar if possible
                if hasattr(self.parent(), 'status_bar'):
                    self.parent().status_bar.showMessage(f"Image saved to {filename}", 3000)
            except Exception as e:
                QMessageBox.critical(self, "Save Error", f"Could not save image:\n{e}")
                logger.error(f"Error saving plot from dialog: {e}", exc_info=True)


class FigureLabel(QLabel):
    """Custom label for figures that can be clicked to enlarge"""
    
    # Signal when clicked
    clicked = Signal(QPixmap, str)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.title = "Plot Viewer"
        self.setCursor(Qt.PointingHandCursor)  # Change cursor to hand when hovering
    
    def mousePressEvent(self, event):
        """Handle mouse press events"""
        if event.button() == Qt.LeftButton and self.pixmap() and not self.pixmap().isNull():
            self.clicked.emit(self.pixmap(), self.title)
        super().mousePressEvent(event)
    
    def setTitle(self, title):
        """Set the title for the plot viewer"""
        self.title = title


class ClientGUI(QMainWindow):
    """Main GUI class for the client application"""
    
    def __init__(self):
        super().__init__()

        # --- Create the Client object *first* ---
        self.client = Client()
        # -----------------------------------------

        self.settings = QSettings("ArrestDataApp", "Client/AppSettings") # For theme persistence
        self.message_check_timer = QTimer(self)
        self.callbacks_bridge = ClientCallbacksBridge()
        self.plot_dialog = None # To store reference to plot dialog
        self.tab_reset_handlers = {} # <--- ADD THIS LINE

        # Load or default theme
        self.current_theme = self.settings.value("theme", "dark") # Default to dark
        
        self.setup_ui()
        self.setup_callbacks()
        self.connect_signals()
        self.apply_theme() # Apply initial theme
        self.update_ui_state() # Initial UI state based on connection/login status

    def setup_ui(self):
        """Set up the UI for the main window"""
        # Set window properties
        self.setWindowTitle("Arrest Data Client")
        self.setMinimumSize(800, 600)
        
        # Create central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Main layout
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(5, 5, 5, 5)  # Reduce margins
        main_layout.setSpacing(5)  # Reduce spacing between elements
        
        # Connection controls and theme toggle in one row
        header_layout = QHBoxLayout()
        header_layout.setSpacing(5)  # Reduce spacing
        
        # Server host input
        self.host_edit = QLineEdit(SERVER_HOST)
        header_layout.addWidget(QLabel("Server:"))
        header_layout.addWidget(self.host_edit)
        
        # Server port input
        self.port_spinbox = QSpinBox()
        self.port_spinbox.setRange(1, 65535)
        self.port_spinbox.setValue(SERVER_PORT)
        header_layout.addWidget(QLabel("Port:"))
        header_layout.addWidget(self.port_spinbox)
        
        # Connect/disconnect button
        self.connect_button = QPushButton("Connect")
        self.connect_button.clicked.connect(self.toggle_connection)
        header_layout.addWidget(self.connect_button)
        
        # Add spacer to push theme toggle to the right
        header_layout.addStretch(1)
        
        # Theme toggle checkbox
        self.theme_toggle = QCheckBox("Dark Theme")
        self.theme_toggle.setChecked(self.current_theme == "dark")
        self.theme_toggle.stateChanged.connect(self.toggle_theme)
        header_layout.addWidget(self.theme_toggle)
        
        # Add connection layout to main layout
        main_layout.addLayout(header_layout)
        
        # Create stack widget for login/register and main app
        self.stack_widget = QTabWidget()
        
        # Login widget
        self.login_widget = LoginWidget()
        self.login_widget.login_button.clicked.connect(self.login)
        self.login_widget.register_button.clicked.connect(self.show_register)
        
        # Register widget
        self.register_widget = RegisterWidget()
        self.register_widget.register_button.clicked.connect(self.register)
        self.register_widget.back_button.clicked.connect(self.show_login)
        
        # Main app tabs
        self.main_tabs = QTabWidget()
        
        # Query tab
        self.query_tab = QueryWidget()
        self.main_tabs.addTab(self.query_tab, "Query")
        
        # Messages tab
        self.message_widget = MessageWidget()
        self.main_tabs.addTab(self.message_widget, "Messages")
        
        # Add login and main tabs to stack widget
        self.stack_widget.addTab(self.login_widget, "Login")
        self.stack_widget.addTab(self.register_widget, "Register")
        self.stack_widget.addTab(self.main_tabs, "Main")
        
        # Initially hide register and main tabs
        self.stack_widget.setTabVisible(1, False)  # Hide register tab
        self.stack_widget.setTabVisible(2, False)  # Hide main tab
        
        # Add stack widget to main layout
        main_layout.addWidget(self.stack_widget)
        
        # Status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Not connected")
        
        # Initialize UI state
        self.update_ui_state()
        self.update_query_params()

    def apply_theme(self):
        """Apply the current theme to the application"""
        if self.current_theme == "dark":
            QApplication.instance().setStyleSheet(DARK_STYLESHEET)
            # Fix tab text color for notifications when using dark theme
            for key in self.tab_reset_handlers.keys():
                tab_widget, tab_index = key
                tab_widget.tabBar().setTabTextColor(tab_index, Qt.red)
        else:
            QApplication.instance().setStyleSheet(LIGHT_STYLESHEET)
            # Reset tab text colors for notifications when using light theme 
            for key in self.tab_reset_handlers.keys():
                tab_widget, tab_index = key
                tab_widget.tabBar().setTabTextColor(tab_index, Qt.red)
    
    def toggle_theme(self):
        """Toggle between light and dark themes"""
        self.current_theme = "dark" if self.theme_toggle.isChecked() else "light"
        # Save the setting
        self.settings.setValue("theme", self.current_theme)
        # Apply the theme
        self.apply_theme()
    
    def connect_signals(self):
        """Connect signals from UI elements and callbacks bridge"""
        # Connection
        # self.connect_button.clicked.connect(self.toggle_connection) # Already connected in setup_ui
        self.callbacks_bridge.connection_status_changed.connect(self.on_connection_status_change)
        
        # Login/Register
        # self.login_widget.login_button.clicked.connect(self.login) # REMOVED - Already connected in setup_ui
        self.login_widget.register_button.clicked.connect(self.show_register)
        # self.register_widget.register_button.clicked.connect(self.register) # REMOVED - Already connected in setup_ui
        self.register_widget.back_button.clicked.connect(self.show_login)
        self.callbacks_bridge.login_status_changed.connect(self.on_login_status_change)
        
        # Query
        self.query_tab.send_query_button.clicked.connect(self.send_query)
        # Connect the new plot_clicked signal from QueryWidget
        self.query_tab.plot_clicked.connect(self.on_plot_clicked)
        self.callbacks_bridge.query_result_received.connect(self.on_query_result)
        
        # Messages
        # self.message_widget.send_button.clicked.connect(self.send_message) # Already commented out
        self.message_widget.clear_button.clicked.connect(self.message_widget.clear_messages)
        self.callbacks_bridge.message_received.connect(self.on_message_received)
        self.message_check_timer.timeout.connect(self.check_messages)

        # Errors
        self.callbacks_bridge.error_occurred.connect(self.on_error)

        # Theme toggle
        # self.theme_toggle.stateChanged.connect(self.toggle_theme) # Already connected in setup_ui

        # Connect FigureLabel clicks from QueryWidget
        # if hasattr(self.query_tab, 'plot_label'): # Duplicate check, removed
        #      self.query_tab.plot_label.clicked.connect(self.on_figure_clicked)

    def setup_callbacks(self):
        """Set up callbacks for the client to use the bridge"""
        # Connection status callback
        self.client.on_connection_status_change = self.callbacks_bridge.on_connection_status_change
        
        # Login status callback
        self.client.on_login_status_change = self.callbacks_bridge.on_login_status_change
        
        # Query result callback
        self.client.on_query_result = self.callbacks_bridge.on_query_result
        
        # Message received callback
        self.client.on_message_received = self.callbacks_bridge.on_message_received
        
        # Error callback
        self.client.on_error = self.callbacks_bridge.on_error
    
    def update_ui_state(self):
        """Update UI state based on connection and login status"""
        connected = self.client.connected
        logged_in = self.client.logged_in
        
        # Update connection button
        if connected:
            self.connect_button.setText("Disconnect")
        else:
            self.connect_button.setText("Connect")
        
        # Update host and port inputs
        self.host_edit.setEnabled(not connected)
        self.port_spinbox.setEnabled(not connected)
        
        # Update stack widget visibility
        self.stack_widget.setTabVisible(0, connected and not logged_in)  # Login tab
        self.stack_widget.setTabVisible(1, False)  # Always hide register tab
        self.stack_widget.setTabVisible(2, connected and logged_in)  # Main tab
        
        # Set current tab based on status
        if connected:
            if logged_in:
                self.stack_widget.setCurrentIndex(2)  # Main tab
            else:
                self.stack_widget.setCurrentIndex(0)  # Login tab
        
        # Update status bar
        if not connected:
            self.status_bar.showMessage("Not connected")
        elif not logged_in:
            self.status_bar.showMessage("Connected, not logged in")
        else:
            nickname = self.client.client_info.get('nickname', 'Unknown')
            self.status_bar.showMessage(f"Logged in as {nickname}")
    
    def update_query_params(self):
        """Fetch dynamic parameters for query dropdowns"""
        logger.info("update_query_params called...")
        if self.client and self.client.connected and self.client.logged_in:
            logger.info("Attempting to fetch dynamic query parameters...")
            try:
                # Fetch Areas
                self.client.send_request({
                    'command': 'get_metadata',
                    'type': 'areas'
                }) # Response handled in on_query_result

                # Fetch Charge Groups
                self.client.send_request({
                    'command': 'get_metadata',
                    'type': 'charge_groups'
                }) # Response handled in on_query_result

                # Fetch Descent Codes
                self.client.send_request({
                    'command': 'get_metadata',
                    'type': 'descent_codes'
                }) # Response handled in on_query_result

                # Fetch Date Range
                self.client.send_request({
                    'command': 'get_metadata',
                    'type': 'date_range'
                }) # Response handled in on_query_result

                # --- ADD REQUEST FOR ARREST TYPE CODES ---
                self.client.send_request({
                    'command': 'get_metadata',
                    'type': 'arrest_type_codes'
                })
                # -----------------------------------------

            except Exception as e:
                logger.error(f"Error sending metadata request: {e}", exc_info=True)
                self.status_bar.showMessage(f"Error fetching query parameters: {e}", 5000)
        else:
            logger.warning("Cannot fetch query parameters: Not connected or not logged in.")
    
    def toggle_connection(self):
        """Toggle connection to the server"""
        # Check the connection state *before* deciding the action
        is_currently_connected = self.client.connected

        if is_currently_connected:
            # Action: Disconnect
            logger.info("TOGGLE_CONNECTION: Currently connected. Attempting disconnect.")
            self.client.disconnect()
            # Callbacks initiated by disconnect() will handle UI updates
        else:
            # Action: Connect
            logger.info("TOGGLE_CONNECTION: Currently disconnected. Attempting connect.")
            host = self.host_edit.text()
            port = self.port_spinbox.value()

            # Update client host and port before connecting
            self.client.host = host
            self.client.port = port

            # Attempt to connect.
            # The connect method itself handles setting flags and calling
            # on_connection_status_change(True/False) via callbacks.
            self.client.connect()
            # No further action needed here; UI updates are handled by callbacks.
    
    def on_connection_status_change(self, connected):
        """Called when connection status changes"""
        self.update_ui_state()
    
    def on_login_status_change(self, logged_in):
        """Called when login status changes"""
        logger.info(f"Login status changed. Logged in: {logged_in}") # Add log
        self.update_ui_state()
        if logged_in:
             logger.info("Login successful, triggering parameter update.")
             self.update_query_params() # Fetch metadata AFTER successful login
    
    def on_query_result(self, result):
        """Handle results received from the server"""
        logger.info(f"Received query result: {type(result)}")

        # Check if the result is for metadata
        metadata_type = result.get('metadata_type')
        if metadata_type:
            self.handle_metadata_result(metadata_type, result.get('data'))
            return # Stop further processing

        # Check if the result is a map (Query 4)
        map_filepath = result.get('map_filepath')
        if map_filepath:
            self.query_tab.clear_results() # Clear table/plot
            # Ensure path is absolute and exists
            if os.path.isabs(map_filepath) and os.path.exists(map_filepath):
                 map_url = Path(map_filepath).as_uri()
                 logger.info(f"Opening map: {map_url}")
                 QMessageBox.information(self, "Map Generated", 
                                         f"Map saved to:\n{map_filepath}\n\nOpening in web browser...")
                 webbrowser.open(map_url)
            else:
                 logger.error(f"Received invalid map filepath: {map_filepath}")
                 self.on_error(f"Server returned an invalid map file path.")
            return # Stop further processing
            
        # Check if the result contains a plot Figure object
        fig_plot = result.get('plot')
        if fig_plot:
            # Check if it's actually a Figure object (Pickle should preserve type)
            from matplotlib.figure import Figure
            if isinstance(fig_plot, Figure):
                try:
                    # No need to decode, pass the Figure object directly
                    plot_title = result.get('title', 'Query Plot')
                    self.query_tab.display_plot(fig_plot, plot_title)
                    logger.info("Query plot Figure object received and sent to display.")
                except Exception as e:
                    logger.error(f"Error displaying plot Figure: {e}", exc_info=True)
                    self.on_error(f"Client error displaying plot: {e}")
                # Return because we only want to show the plot
                return 
            else:
                logger.error(f"Received 'plot' data is not a Matplotlib Figure. Type: {type(fig_plot)}")
                self.on_error("Client error: Received invalid plot data type from server.")
                # Fall through to potentially display table data if available?
                # Or return here as well if plot was expected but invalid?
                # Let's fall through for now.
            
        # Check if the result contains data for the table (and no plot was handled)
        if 'data' in result:
            # This block is now only reached if fig_plot was None/False
            self.query_tab.display_results(result)
            logger.info("Query data result received and sent to display.")
        else:
            # Handle case where neither plot nor data is present 
            logger.warning("Query result received with OK status but no data or plot.")
            self.query_tab.display_results(result) # Will show 'No results'

    def handle_metadata_result(self, metadata_type, data):
        """Update combo boxes or date edits with metadata received from server"""
        logger.info(f"Handling metadata for: {metadata_type}")

        if metadata_type == 'areas':
            if not isinstance(data, list):
                 logger.warning(f"Received non-list data for metadata {metadata_type}")
                 return
            # --- Update ONLY Q1 area combo box ---
            area_combo_q1 = self.query_tab.q1_area_combo
            # area_combo_q4 = self.query_tab.q4_area_combo # Remove reference to Q4 combo
            area_combo_q1.clear()
            # area_combo_q4.clear()
            if data:
                items = [str(item) for item in data]
                area_combo_q1.addItems(items)
                # area_combo_q4.addItems(items) # Don't populate Q4 combo
            else:
                area_combo_q1.addItem("No areas found")
                # area_combo_q4.addItem("No areas found")
            # ------------------------------------
        elif metadata_type == 'charge_groups':
            if not isinstance(data, list):
                 logger.warning(f"Received non-list data for metadata {metadata_type}")
                 return
            q2_charge_combo = self.query_tab.q2_charge_combo
            q3_charge_combo = self.query_tab.q3_charge_combo
            q2_charge_combo.clear()
            q3_charge_combo.clear()
            q3_charge_combo.addItem("Optional: All Types") # Add default option for Q3
            if data:
                 items = [str(item) for item in data]
                 q2_charge_combo.addItems(items)
                 q3_charge_combo.addItems(items)
            else:
                 q2_charge_combo.addItem("No charge types found")
                 q3_charge_combo.addItem("No charge types found")
        elif metadata_type == 'descent_codes':
            list_widget = self.query_tab.q3_descent_list
            list_widget.clear() 
            if isinstance(data, list) and data:
                for item_data in data:
                    if isinstance(item_data, dict):
                        code = item_data.get('code')
                        desc = item_data.get('description', code) 
                        if code:
                             list_item = QListWidgetItem(f"{desc} ({code})")
                             list_item.setData(Qt.UserRole, code)
                             list_item.setFlags(list_item.flags() | Qt.ItemIsUserCheckable)
                             list_item.setCheckState(Qt.Unchecked)
                             list_widget.addItem(list_item)
                    else:
                        logger.warning(f"Ignoring invalid item format in descent_codes metadata: {item_data}")
            else:
                 list_item = QListWidgetItem("No descent codes available")
                 list_item.setFlags(list_item.flags() & ~Qt.ItemIsEnabled) 
                 list_widget.addItem(list_item)
        elif metadata_type == 'date_range':
             if not isinstance(data, dict):
                  logger.warning(f"Received non-dict data for metadata {metadata_type}")
                  return
             try:
                  min_date_str = data.get('min_date')
                  max_date_str = data.get('max_date')

                  # Use QDate.currentDate() as fallback if parsing fails or data missing
                  min_qdate = QDate.fromString(min_date_str.split('T')[0], Qt.ISODate) if min_date_str else QDate.currentDate()
                  max_qdate = QDate.fromString(max_date_str.split('T')[0], Qt.ISODate) if max_date_str else QDate.currentDate()

                  # Ensure min_qdate is not invalid (QDate() is invalid)
                  if not min_qdate.isValid():
                       min_qdate = QDate.currentDate()
                  if not max_qdate.isValid():
                       max_qdate = QDate.currentDate()

                  logger.info(f"Processing date range: Min={min_qdate.toString(Qt.ISODate)}, Max={max_qdate.toString(Qt.ISODate)}")

                  # --- Explicitly set ranges and dates for each widget ---
                  # Query 1 Widgets
                  if hasattr(self.query_tab, 'q1_start_date'):
                      self.query_tab.q1_start_date.setMinimumDate(min_qdate)
                      self.query_tab.q1_start_date.setMaximumDate(max_qdate)
                      self.query_tab.q1_start_date.setDate(min_qdate) # Default start to min
                      logger.debug(f"Set q1_start_date: Range [{min_qdate.toString(Qt.ISODate)} - {max_qdate.toString(Qt.ISODate)}], Value: {min_qdate.toString(Qt.ISODate)}")

                  if hasattr(self.query_tab, 'q1_end_date'):
                      self.query_tab.q1_end_date.setMinimumDate(min_qdate)
                      self.query_tab.q1_end_date.setMaximumDate(max_qdate)
                      self.query_tab.q1_end_date.setDate(max_qdate) # Default end to max
                      logger.debug(f"Set q1_end_date: Range [{min_qdate.toString(Qt.ISODate)} - {max_qdate.toString(Qt.ISODate)}], Value: {max_qdate.toString(Qt.ISODate)}")

                  # Query 4 Widgets
                  if hasattr(self.query_tab, 'q4_start_date'):
                      self.query_tab.q4_start_date.setMinimumDate(min_qdate)
                      self.query_tab.q4_start_date.setMaximumDate(max_qdate)
                      self.query_tab.q4_start_date.setDate(min_qdate) # Default start to min
                      logger.debug(f"Set q4_start_date: Range [{min_qdate.toString(Qt.ISODate)} - {max_qdate.toString(Qt.ISODate)}], Value: {min_qdate.toString(Qt.ISODate)}")

                  if hasattr(self.query_tab, 'q4_end_date'):
                      self.query_tab.q4_end_date.setMinimumDate(min_qdate)
                      self.query_tab.q4_end_date.setMaximumDate(max_qdate)
                      self.query_tab.q4_end_date.setDate(max_qdate) # Default end to max
                      logger.debug(f"Set q4_end_date: Range [{min_qdate.toString(Qt.ISODate)} - {max_qdate.toString(Qt.ISODate)}], Value: {max_qdate.toString(Qt.ISODate)}")
                  # -------------------------------------------------------

                  logger.info(f"Successfully set date range and values for date editors.")

             except Exception as e:
                  logger.error(f"Error processing date_range metadata: {e}", exc_info=True)
        elif metadata_type == 'arrest_type_codes':
             if not isinstance(data, list):
                  logger.warning(f"Received non-list data for metadata {metadata_type}")
                  return
             arrest_type_combo = self.query_tab.q4_arrest_type_combo
             arrest_type_combo.clear()
             arrest_type_combo.addItem("All Types") # Add default 'All' option
             if data:
                 arrest_type_combo.addItems([str(item) for item in data])
             else:
                 arrest_type_combo.addItem("No types found") # Fallback
             logger.info(f"Populated Arrest Type Code dropdown.")
        else:
            logger.warning(f"Received unknown metadata type: {metadata_type}")

    def on_message_received(self, timestamp, message):
        """Called when message is received from server"""
        # Add message to message widget
        self.message_widget.messages_text.append(f"<span style='color:blue'>[SERVER {timestamp}]</span> {message}")
        
        # Also add to the dedicated server messages area with highlighting (use a brighter color)
        self.message_widget.server_messages_text.append(
            f"<b style='font-size:14px'>[{timestamp}]</b>: "
            f"<span style='color:#00BFFF;font-weight:bold'>{message}</span>"
        )
        
        # Scroll to the bottom to ensure visibility for both text areas
        for text_area in [self.message_widget.messages_text, self.message_widget.server_messages_text]:
            scrollbar = text_area.verticalScrollBar()
            scrollbar.setValue(scrollbar.maximum())
        
        # Show the Messages tab if not currently visible
        if self.main_tabs.currentWidget() != self.message_widget:
            # Flash the tab or show a notification
            tab_index = self.main_tabs.indexOf(self.message_widget)
            if tab_index >= 0:
                # Get the original text without notification indicators
                original_text = self.main_tabs.tabText(tab_index).replace("★ ", "").replace("🔔 ", "")
                self.setup_tab_notification(self.main_tabs, tab_index, original_text)
        
        # Process UI events to make sure the changes are visible immediately
        QApplication.processEvents()
    
    def on_error(self, error_message):
        """Called when error occurs"""
        # Show error message
        QMessageBox.critical(self, "Error", error_message)
    
    def check_messages(self):
        """Check for new messages in queue"""
        if not self.client:
            return
            
        message_received = False
        processed_count = 0
        max_messages_per_check = 10  # Limit number of messages to process at once
        
        while processed_count < max_messages_per_check:
            message = self.client.get_next_message()
            if message is None:
                break
                
            message_received = True
            processed_count += 1
            
            # Format timestamp
            try:
                dt = datetime.fromisoformat(message['timestamp'])
                formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                formatted_time = message['timestamp']
            
            # Print debug info for diagnostic purposes
            print(f"Received message from queue: {message['type']} - {formatted_time} - {message['message']}")
                
            # Handle message based on type
            if message['type'] == 'info':
                self.message_widget.messages_text.append(f"<span style='color:green'>[INFO {formatted_time}]</span> {message['message']}")
            elif message['type'] == 'error':
                self.message_widget.messages_text.append(f"<span style='color:red'>[ERROR {formatted_time}]</span> {message['message']}")
            # For server type, we only process if use_queue flag is set to avoid duplicates
            elif message['type'] == 'server' and message.get('use_queue', False):
                # Add to general messages
                self.message_widget.messages_text.append(f"<span style='color:blue'>[SERVER {formatted_time}]</span> {message['message']}")
                
                # Also add to dedicated server messages area with more visible formatting
                self.message_widget.server_messages_text.append(
                    f"<b style='font-size:14px'>[{formatted_time}]</b>: "
                    f"<span style='color:#00BFFF;font-weight:bold'>{message['message']}</span>"
                )
                
                # Scroll to the bottom to ensure visibility for both text areas
                for text_area in [self.message_widget.messages_text, self.message_widget.server_messages_text]:
                    scrollbar = text_area.verticalScrollBar()
                    scrollbar.setValue(scrollbar.maximum())
                
                # Show the Messages tab if not currently visible
                if self.main_tabs.currentWidget() != self.message_widget:
                    # Set up tab notification
                    tab_index = self.main_tabs.indexOf(self.message_widget)
                    if tab_index >= 0:
                        # Get the original text without notification indicators
                        original_text = self.main_tabs.tabText(tab_index).replace("★ ", "").replace("🔔 ", "")
                        self.setup_tab_notification(self.main_tabs, tab_index, original_text)
        
        # If we received any messages, make sure the UI updates
        if message_received:
            QApplication.processEvents()
    
    def show_login(self):
        """Show login tab"""
        self.stack_widget.setTabVisible(0, True)  # Show login tab
        self.stack_widget.setTabVisible(1, False)  # Hide register tab
        self.stack_widget.setCurrentIndex(0)  # Set current to login tab
    
    def show_register(self):
        """Show register tab"""
        self.stack_widget.setTabVisible(0, False)  # Hide login tab
        self.stack_widget.setTabVisible(1, True)  # Show register tab
        self.stack_widget.setCurrentIndex(1)  # Set current to register tab
    
    def login(self):
        """Login to the server"""
        # Get email and password
        email = self.login_widget.email_edit.text()
        password = self.login_widget.password_edit.text()
        
        # Validate inputs
        if not email or not password:
            QMessageBox.warning(self, "Login", "Email and password are required")
            return
        
        # Send login request
        self.client.login(email, password)
    
    def register(self):
        """Register a new user"""
        # Get registration details
        name = self.register_widget.name_edit.text().strip()
        nickname = self.register_widget.nickname_edit.text().strip()
        email = self.register_widget.email_edit.text().strip()
        password = self.register_widget.password_edit.text().strip()
        confirm_password = self.register_widget.confirm_password_edit.text().strip()

        if not all([name, nickname, email, password]):
            QMessageBox.warning(self, "Registration Failed", "All fields are required.")
            return

        # Basic email format validation using regex
        email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$" # Simple regex
        if not re.match(email_regex, email):
             QMessageBox.warning(self, "Registration Failed", "Invalid email format.")
             return

        if password != confirm_password:
            QMessageBox.warning(self, "Register", "Passwords do not match")
            return
        
        # Send registration request
        success, message = self.client.register(name, nickname, email, password)

        if success:
            # Show login tab (registration response will be handled by callbacks)
            # self.show_login() # <-- REMOVE THIS LINE
            
            # Automatically log in the user after successful registration
            logger.info(f"Registration successful for {email}. Attempting automatic login.")
            self.status_bar.showMessage(f"Registration successful. Logging in as {nickname}...")
            # Directly call the client's login method
            self.client.login(email, password)
            # The on_login_status_change callback will handle the UI transition
        else:
            QMessageBox.critical(self, "Registration Error", message)
    
    def send_query(self):
        """Gather parameters based on selected query and send to server"""
        if not self.client or not self.client.connected or not self.client.logged_in:
            QMessageBox.warning(self, "Not Connected", "Please connect and log in first.")
            return

        query_index = self.query_tab.query_type_combo.currentIndex()
        params = {'command': 'query'}
        query_type_id = f"query{query_index + 1}" # e.g., query1, query2, etc.
        params['query_type'] = query_type_id

        try:
            if query_index == 0: # Query 1
                params['area_name'] = self.query_tab.q1_area_combo.currentText()
                params['start_date'] = self.query_tab.q1_start_date.date().toString(Qt.ISODate)
                params['end_date'] = self.query_tab.q1_end_date.date().toString(Qt.ISODate)
                params['min_age'] = self.query_tab.q1_min_age_spin.value()
                params['max_age'] = self.query_tab.q1_max_age_spin.value()
                if params['min_age'] > params['max_age']:
                    raise ValueError("Minimum age cannot be greater than maximum age.")
            elif query_index == 1: # Query 2
                params['charge_group'] = self.query_tab.q2_charge_combo.currentText()
                params['granularity'] = self.query_tab.q2_granularity_combo.currentText().lower()
                # areas = self.query_tab.q2_area_input.text().strip() # <-- REMOVED
                # params['areas'] = [a.strip() for a in areas.split(',') if a.strip()] if areas else [] # Optional <-- REMOVED
            elif query_index == 2: # Query 3
                sex_codes = []
                if self.query_tab.q3_sex_m_check.isChecked(): sex_codes.append('M')
                if self.query_tab.q3_sex_f_check.isChecked(): sex_codes.append('F')
                if not sex_codes: raise ValueError("Please select at least one gender.")
                params['sex_codes'] = sex_codes

                # --- Get selected descent codes from QListWidget --- 
                selected_descent_codes = []
                list_widget = self.query_tab.q3_descent_list
                for i in range(list_widget.count()):
                     item = list_widget.item(i)
                     # Check if the item exists and is checkable before checking state
                     if item and item.flags() & Qt.ItemIsUserCheckable and item.checkState() == Qt.Checked:
                          code = item.data(Qt.UserRole)
                          if code:
                               selected_descent_codes.append(code)

                if not selected_descent_codes:
                     raise ValueError("Please select at least one descent code.")
                params['descent_codes'] = selected_descent_codes
                # ----------------------------------------------------
                # # REMOVE incorrect logic from previous edit attempt:
                # descents = self.query_tab.q3_descent_list.selectedItems()
                # params['descent_codes'] = [item.text().strip() for item in descents]
                # if not params['descent_codes']: raise ValueError("Please select at least one descent code.")

                charge_group = self.query_tab.q3_charge_combo.currentText()
                params['charge_group'] = charge_group if "Optional:" not in charge_group else None # Optional
                params['generate_plot'] = True # Explicitly request plot
                
            elif query_index == 3: # Query 4
                 # --- Remove area_name, Add center_lat/lon ---
                 # params['area_name'] = self.query_tab.q4_area_combo.currentText()
                 # if "Loading areas..." in params['area_name'] or "No areas found" in params['area_name']:
                 #      raise ValueError("Please select a valid Centrum Gebied.")
                 params['center_lat'] = self.query_tab.q4_center_lat_spin.value()
                 params['center_lon'] = self.query_tab.q4_center_lon_spin.value()
                 # ---------------------------------------------

                 params['radius_km'] = self.query_tab.q4_radius_spin.value()
                 params['start_date'] = self.query_tab.q4_start_date.date().toString(Qt.ISODate)
                 params['end_date'] = self.query_tab.q4_end_date.date().toString(Qt.ISODate)

                 selected_arrest_type = self.query_tab.q4_arrest_type_combo.currentText()
                 params['arrest_type_code'] = selected_arrest_type if selected_arrest_type != "All Types" else None

                 # Note: Server process_query4 needs adjustment to accept center_lat/lon
            else:
                QMessageBox.critical(self, "Error", "Invalid query type selected.")
                return

            logger.info(f"Sending query: {params}")
            self.statusBar().showMessage(f"Sending {query_type_id}...")
            self.query_tab.clear_results() # Clear previous results before sending
            self.client.send_request(params)

        except ValueError as ve:
            QMessageBox.warning(self, "Input Error", str(ve))
            self.statusBar().showMessage(f"Input Error: {ve}", 5000)
        except Exception as e:
            logger.error(f"Error preparing or sending query: {e}")
            QMessageBox.critical(self, "Query Error", f"An error occurred: {e}")
            self.statusBar().showMessage(f"Query Error: {e}", 5000)
    
    # SLot to handle plot clicks from QueryWidget
    @Slot(object) # Type hint for the Figure object
    def on_plot_clicked(self, fig):
        """Handle the plot_clicked signal from QueryWidget."""
        if fig is None:
            logger.warning("on_plot_clicked received None figure.")
            return
        
        # Get title from figure if possible, or use default
        # Matplotlib Figure objects don't have a simple .title attribute
        # We might need to get it from the axes, or pass it separately.
        # For now, use a generic title.
        # TODO: Find a way to get/pass the plot title for the dialog.
        dialog_title = "Plot Viewer"
        
        # Create and show the plot viewer dialog
        try:
            dialog = PlotViewerDialog(fig, dialog_title, self) # Pass self as parent
            dialog.exec() # Use exec() for modal dialog
        except Exception as e:
            logger.error(f"Error creating or showing PlotViewerDialog: {e}", exc_info=True)
            QMessageBox.critical(self, "Error", f"Could not open plot viewer: {e}")

    def closeEvent(self, event):
        """Called when window is closed"""
        # 1) Disconnect socket and stop the receive-loop
        if self.client.connected:
            self.client.disconnect()

        # 2) Stop our polling timer (if running)
        self.message_check_timer.stop()

        # 3) Clear out any callbacks so the receive thread can't call into dead objects
        self.client.on_connection_status_change = None
        self.client.on_login_status_change = None
        self.client.on_message_received = None
        self.client.on_query_result = None
        self.client.on_error = None

        # 4) Wait briefly for the receive thread to finish
        if self.client.receiver_thread and self.client.receiver_thread.is_alive():
            self.client.receiver_thread.join(timeout=1.0)

        # 5) Now accept and let Qt tear down the window safely
        event.accept()

    def setup_tab_notification(self, tab_widget, tab_index, original_text):
        """Set up a notification on a tab with proper connection handling"""
        # Use a simple indicator, e.g., a star or bell
        notification_prefix = "🔔 "
        tab_widget.setTabText(tab_index, f"{notification_prefix}{original_text}")
        tab_widget.tabBar().setTabTextColor(tab_index, Qt.red) # Use red for notifications

        # --- Handler function to reset notification ---
        def reset_tab_notification():
            # Check if the notified tab is the one being switched to
            if tab_widget.currentWidget() == tab_widget.widget(tab_index):
                tab_widget.setTabText(tab_index, original_text) # Reset text

                # Reset color based on theme
                text_color = Qt.white if self.current_theme == "dark" else Qt.black
                # For selected tab, use theme's selected color if available
                if tab_widget.currentIndex() == tab_index:
                     if self.current_theme == "dark":
                          # Use the selected tab color from the stylesheet or a fallback
                          text_color = QColor("#FFFFFF") # White for dark theme selected
                     else:
                          text_color = QColor("#FFFFFF") # Often white for light theme selected too

                # Explicitly set the color (might be overridden by stylesheet selection)
                # This part might need refinement depending on exact stylesheet behavior
                # tab_widget.tabBar().setTabTextColor(tab_index, text_color)


                # Clean up the handler
                key = (tab_widget, tab_index)
                if key in self.tab_reset_handlers:
                    # Disconnect the specific handler
                    try:
                         handler_to_disconnect = self.tab_reset_handlers.pop(key)
                         tab_widget.currentChanged.disconnect(handler_to_disconnect)
                         logger.debug(f"Disconnected notification reset handler for tab {tab_index}")
                    except (TypeError, RuntimeError) as e:
                         logger.warning(f"Could not disconnect handler for tab {tab_index}: {e}")

        # --- Store and connect the handler ---
        key = (tab_widget, tab_index)
        # Disconnect previous handler for this specific tab if it exists
        if key in self.tab_reset_handlers:
            try:
                old_handler = self.tab_reset_handlers[key]
                tab_widget.currentChanged.disconnect(old_handler)
                logger.debug(f"Disconnected old handler for tab {tab_index} before adding new one.")
            except (TypeError, RuntimeError) as e:
                logger.warning(f"Could not disconnect old handler for tab {tab_index}: {e}")

        # Store and connect the new handler
        self.tab_reset_handlers[key] = reset_tab_notification
        try:
            tab_widget.currentChanged.connect(reset_tab_notification)
            logger.debug(f"Connected notification reset handler for tab {tab_index}")
        except Exception as e:
            logger.error(f"Failed to connect notification handler: {e}")


if __name__ == "__main__":
    # Create application
    app = QApplication(sys.argv)
    
    # Load settings to check for initial theme
    settings = QSettings("ArrestDataApp", "Client/AppSettings")
    # Fetch the theme setting key used in ClientGUI.__init__ which is 'theme'
    current_theme = settings.value("theme", "dark") # Default to dark
    
    # Apply initial theme before creating the UI
    if current_theme == "dark":
        app.setStyleSheet(DARK_STYLESHEET)
    else:
        app.setStyleSheet(LIGHT_STYLESHEET)
    
    # Create and show main window
    main_window = ClientGUI()
    main_window.show()
    
    # Run application
    sys.exit(app.exec()) 