#!/usr/bin/env python3
# GUI Client for the arrest data client-server application

import os
import sys
import time
import logging
from datetime import datetime
from PIL import Image, ImageQt
import io

# Add the parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import client module
from client.client import Client

# Import shared modules
from shared.constants import *

# Import PySide6 modules
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QTabWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QTextEdit, QComboBox, QSpinBox, QGroupBox,
    QFormLayout, QSplitter, QTableWidget, QTableWidgetItem, QMessageBox,
    QStatusBar, QScrollArea, QGridLayout, QDialog, QFileDialog, QCheckBox
)
from PySide6.QtGui import QPixmap, QFont, QIcon, QPalette, QColor
from PySide6.QtCore import Qt, QTimer, Signal, Slot, QObject, QSettings

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('client_gui')

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

QComboBox::drop-down {
    border: none;
    border-left: 1px solid #3F3F46;
}

QComboBox QAbstractItemView {
    background-color: #1E1E1E;
    color: #E1E1E1;
    selection-background-color: #007ACC;
}

QTableWidget {
    background-color: #1E1E1E;
    alternate-background-color: #2D2D30;
    color: #E1E1E1;
    gridline-color: #3F3F46;
    selection-background-color: #007ACC;
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
"""

# Light theme stylesheet - default Qt style with some customizations
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

QStatusBar {
    background-color: #007ACC;
    color: white;
}
"""

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


class LoginWidget(QWidget):
    """Login widget for the client GUI"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Set up the UI
        self.setup_ui()
    
    def setup_ui(self):
        """Set up the UI for the login widget"""
        # Main layout
        layout = QVBoxLayout(self)
        
        # Form layout for login fields
        form_layout = QFormLayout()
        
        # Email field
        self.email_edit = QLineEdit()
        form_layout.addRow("Email:", self.email_edit)
        
        # Password field
        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.Password)
        form_layout.addRow("Password:", self.password_edit)
        
        # Add form layout to main layout
        layout.addLayout(form_layout)
        
        # Add some space
        layout.addSpacing(20)
        
        # Login button
        self.login_button = QPushButton("Login")
        layout.addWidget(self.login_button)
        
        # Register button
        self.register_button = QPushButton("Register")
        layout.addWidget(self.register_button)
        
        # Add stretch to push buttons to the top
        layout.addStretch()


class RegisterWidget(QWidget):
    """Register widget for the client GUI"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Set up the UI
        self.setup_ui()
    
    def setup_ui(self):
        """Set up the UI for the register widget"""
        # Main layout
        layout = QVBoxLayout(self)
        
        # Form layout for registration fields
        form_layout = QFormLayout()
        
        # Name field
        self.name_edit = QLineEdit()
        form_layout.addRow("Name:", self.name_edit)
        
        # Nickname field
        self.nickname_edit = QLineEdit()
        form_layout.addRow("Nickname:", self.nickname_edit)
        
        # Email field
        self.email_edit = QLineEdit()
        form_layout.addRow("Email:", self.email_edit)
        
        # Password field
        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.Password)
        form_layout.addRow("Password:", self.password_edit)
        
        # Confirm password field
        self.confirm_password_edit = QLineEdit()
        self.confirm_password_edit.setEchoMode(QLineEdit.Password)
        form_layout.addRow("Confirm Password:", self.confirm_password_edit)
        
        # Add form layout to main layout
        layout.addLayout(form_layout)
        
        # Add some space
        layout.addSpacing(20)
        
        # Register button
        self.register_button = QPushButton("Register")
        layout.addWidget(self.register_button)
        
        # Back button
        self.back_button = QPushButton("Back to Login")
        layout.addWidget(self.back_button)
        
        # Add stretch to push buttons to the top
        layout.addStretch()


class QueryWidget(QWidget):
    """Query widget for the client GUI"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Set up the UI
        self.setup_ui()
    
    def setup_ui(self):
        """Set up the UI for the query widget"""
        # Main layout
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(5, 5, 5, 5)  # Reduce margins
        main_layout.setSpacing(5)  # Reduce spacing
        
        # Create a group box for query input
        query_group = QGroupBox("Query Parameters")
        query_layout = QFormLayout()
        query_layout.setContentsMargins(5, 5, 5, 5)  # Reduce margins
        query_layout.setSpacing(5)  # Reduce spacing
        
        # Query type
        self.query_type_combo = QComboBox()
        self.query_type_combo.setMinimumWidth(300)  # Make the combo box wider
        for query_type, description in QUERY_DESCRIPTIONS.items():
            self.query_type_combo.addItem(description, query_type)
        query_layout.addRow("Query Type:", self.query_type_combo)
        
        # Help text for queries
        self.query_help_text = QTextEdit()
        self.query_help_text.setReadOnly(True)
        self.query_help_text.setMaximumHeight(60)  # Reduce height
        self.query_help_text.setPlaceholderText("Select a query type to see help")
        query_layout.addRow("Query Help:", self.query_help_text)
        
        # Query parameters that change based on query type
        self.parameters_widget = QWidget()
        self.parameters_layout = QFormLayout(self.parameters_widget)
        self.parameters_layout.setContentsMargins(0, 0, 0, 0)  # Remove margins
        self.parameters_layout.setSpacing(5)  # Reduce spacing
        
        # N parameter for top N queries
        self.n_spinbox = QSpinBox()
        self.n_spinbox.setRange(1, 100)
        self.n_spinbox.setValue(10)
        self.parameters_layout.addRow("Top N:", self.n_spinbox)
        
        # Year parameter for time-based queries
        self.year_spinbox = QSpinBox()
        self.year_spinbox.setRange(2010, 2023)
        self.year_spinbox.setValue(2020)
        self.year_spinbox.setSpecialValueText("All Years")
        self.parameters_layout.addRow("Year:", self.year_spinbox)
        
        # Gender parameter
        self.gender_combo = QComboBox()
        self.gender_combo.addItem("All", "all")
        self.gender_combo.addItem("Male", "M")
        self.gender_combo.addItem("Female", "F")
        self.parameters_layout.addRow("Gender:", self.gender_combo)
        
        # Age range parameters
        self.min_age_spinbox = QSpinBox()
        self.min_age_spinbox.setRange(0, 100)
        self.min_age_spinbox.setValue(18)
        self.parameters_layout.addRow("Min Age:", self.min_age_spinbox)
        
        self.max_age_spinbox = QSpinBox()
        self.max_age_spinbox.setRange(0, 100)
        self.max_age_spinbox.setValue(65)
        self.parameters_layout.addRow("Max Age:", self.max_age_spinbox)
        
        query_layout.addRow("Parameters:", self.parameters_widget)
        
        # Send query button
        self.send_query_button = QPushButton("Send Query")
        self.send_query_button.setMinimumHeight(30)  # Make button taller for better visibility
        query_layout.addRow("", self.send_query_button)
        
        # Set layout for query group
        query_group.setLayout(query_layout)
        
        # Add query group to main layout
        main_layout.addWidget(query_group)
        
        # Results section
        results_group = QGroupBox("Query Results")
        results_layout = QVBoxLayout()
        results_layout.setContentsMargins(5, 5, 5, 5)  # Reduce margins
        results_layout.setSpacing(5)  # Reduce spacing
        
        # Results title
        self.results_title = QLabel("No results yet")
        self.results_title.setStyleSheet("font-weight: bold; font-size: 14px;")
        results_layout.addWidget(self.results_title)
        
        # Create a splitter for table and figures
        splitter = QSplitter(Qt.Vertical)
        
        # Table widget for data results
        self.results_table = QTableWidget()
        self.results_table.setAlternatingRowColors(True)  # Better readability
        splitter.addWidget(self.results_table)
        
        # Scroll area for figures
        figures_scroll = QScrollArea()
        figures_scroll.setWidgetResizable(True)
        figures_widget = QWidget()
        self.figures_layout = QVBoxLayout(figures_widget)
        self.figures_layout.setContentsMargins(5, 5, 5, 5)  # Reduce margins
        self.figures_layout.setSpacing(5)  # Reduce spacing
        
        # Labels for figures - use FigureLabel instead of QLabel
        self.figure1_label = FigureLabel()
        self.figure1_label.setAlignment(Qt.AlignCenter)
        self.figure1_label.setText("No figure available")
        self.figure1_label.setTitle("Figure 1")
        self.figures_layout.addWidget(self.figure1_label)
        
        self.figure2_label = FigureLabel()
        self.figure2_label.setAlignment(Qt.AlignCenter)
        self.figure2_label.setText("No figure available")
        self.figure2_label.setTitle("Figure 2")
        self.figures_layout.addWidget(self.figure2_label)
        
        figures_scroll.setWidget(figures_widget)
        splitter.addWidget(figures_scroll)
        
        # Add the splitter to the results layout
        results_layout.addWidget(splitter)
        
        # Set the layout for the results group
        results_group.setLayout(results_layout)
        
        # Add results group to main layout
        main_layout.addWidget(results_group)


class MessageWidget(QWidget):
    """Widget for displaying messages"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Set up layout
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)  # Reduce margins
        layout.setSpacing(5)  # Reduce spacing
        
        # Create title label for all messages
        self.all_messages_label = QLabel("All Messages:")
        self.all_messages_label.setStyleSheet("font-weight: bold; font-size: 12px;")
        layout.addWidget(self.all_messages_label)
        
        # Create message display
        self.messages_text = QTextEdit()
        self.messages_text.setReadOnly(True)
        self.messages_text.setMinimumHeight(120)  # Reduce minimum height
        layout.addWidget(self.messages_text)
        
        # Add a visible server messages section with a more attention-grabbing style
        self.server_message_label = QLabel("Server Messages:")
        self.server_message_label.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(self.server_message_label)
        
        # Server messages display with improved visibility
        self.server_messages_text = QTextEdit()
        self.server_messages_text.setReadOnly(True)
        self.server_messages_text.setMinimumHeight(150)  # Make this area larger but still more compact
        layout.addWidget(self.server_messages_text)
        
        # Add a "Clear Messages" button in a horizontal layout to save space
        button_layout = QHBoxLayout()
        self.clear_button = QPushButton("Clear All Messages")
        button_layout.addStretch()
        button_layout.addWidget(self.clear_button)
        self.clear_button.clicked.connect(self.clear_messages)
        layout.addLayout(button_layout)
        
        self.setLayout(layout)
    
    def clear_messages(self):
        """Clear all message displays"""
        self.messages_text.clear()
        self.server_messages_text.clear()


class PlotViewerDialog(QDialog):
    """Dialog for viewing plots in a larger size"""
    
    def __init__(self, pixmap, title, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        
        # Set a large initial size (80% of screen size)
        screen_size = QApplication.primaryScreen().size()
        self.resize(int(screen_size.width() * 0.8), int(screen_size.height() * 0.8))
        
        # Create layout
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)  # Reduce margins
        layout.setSpacing(5)  # Reduce spacing
        
        # Create scroll area for the plot
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        
        # Create label for the plot
        self.plot_label = QLabel()
        self.plot_label.setAlignment(Qt.AlignCenter)
        self.plot_label.setPixmap(pixmap)
        self.plot_label.setScaledContents(False)  # Don't scale automatically
        
        # Add label to scroll area
        scroll_area.setWidget(self.plot_label)
        
        # Add scroll area to layout
        layout.addWidget(scroll_area, 1)
        
        # Create buttons
        button_layout = QHBoxLayout()
        button_layout.setSpacing(5)  # Reduce spacing
        
        # Save button
        self.save_button = QPushButton("Save Image")
        self.save_button.setIcon(QIcon.fromTheme("document-save"))  # Add icon if available
        self.save_button.clicked.connect(self.save_image)
        button_layout.addWidget(self.save_button)
        
        # Add spacer to push close button to the right
        button_layout.addStretch()
        
        # Close button
        self.close_button = QPushButton("Close")
        self.close_button.setIcon(QIcon.fromTheme("window-close"))  # Add icon if available
        self.close_button.clicked.connect(self.accept)
        button_layout.addWidget(self.close_button)
        
        # Add buttons to layout
        layout.addLayout(button_layout)
        
        # Store the pixmap for saving
        self.pixmap = pixmap
    
    def save_image(self):
        """Save the plot image to a file"""
        filename, _ = QFileDialog.getSaveFileName(
            self, "Save Image", "", "Images (*.png *.jpg *.jpeg)"
        )
        if filename:
            if not filename.lower().endswith(('.png', '.jpg', '.jpeg')):
                filename += '.png'
            self.pixmap.save(filename)
            
            # Show confirmation in status bar if we can access parent's status bar
            if hasattr(self.parent(), 'status_bar'):
                self.parent().status_bar.showMessage(f"Image saved to {filename}", 3000)


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
    """Main window for the client GUI"""
    
    def __init__(self):
        super().__init__()
        
        # Create client callback bridge
        self.callback_bridge = ClientCallbacksBridge()
        
        # Create client
        self.client = Client()
        
        # Store notification reset handlers
        self.tab_reset_handlers = {}
        
        # Initialize settings
        self.settings = QSettings("ArrestDataClient", "AppSettings")
        
        # Default theme to dark
        self.dark_theme = self.settings.value("dark_theme", True, type=bool)
        
        # Set up UI
        self.setup_ui()
        
        # Apply initial theme
        self.apply_theme()
        
        # Connect signals from bridge to slots
        self.connect_signals()
        
        # Set up client callbacks to use the bridge
        self.setup_callbacks()
        
        # Set up timer for checking messages
        self.message_timer = QTimer(self)
        self.message_timer.timeout.connect(self.check_messages)
        self.message_timer.start(100)  # Check messages every 100ms
    
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
        self.theme_toggle.setChecked(self.dark_theme)
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
        self.query_widget = QueryWidget()
        self.query_widget.query_type_combo.currentIndexChanged.connect(self.update_query_params)
        self.query_widget.send_query_button.clicked.connect(self.send_query)
        self.main_tabs.addTab(self.query_widget, "Query")
        
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
        if self.dark_theme:
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
        self.dark_theme = self.theme_toggle.isChecked()
        # Save the setting
        self.settings.setValue("dark_theme", self.dark_theme)
        # Apply the theme
        self.apply_theme()
    
    def connect_signals(self):
        """Connect signals from bridge to slots"""
        # Connection status
        self.callback_bridge.connection_status_changed.connect(self.on_connection_status_change)
        
        # Login status
        self.callback_bridge.login_status_changed.connect(self.on_login_status_change)
        
        # Messages
        self.callback_bridge.message_received.connect(self.on_message_received)
        
        # Query results
        self.callback_bridge.query_result_received.connect(self.on_query_result)
        
        # Error
        self.callback_bridge.error_occurred.connect(self.on_error)
        
        # Connect the figure labels' clicked signals
        self.query_widget.figure1_label.clicked.connect(self.on_figure_clicked)
        self.query_widget.figure2_label.clicked.connect(self.on_figure_clicked)
    
    def setup_callbacks(self):
        """Set up callbacks for the client to use the bridge"""
        # Connection status callback
        self.client.on_connection_status_change = self.callback_bridge.on_connection_status_change
        
        # Login status callback
        self.client.on_login_status_change = self.callback_bridge.on_login_status_change
        
        # Query result callback
        self.client.on_query_result = self.callback_bridge.on_query_result
        
        # Message received callback
        self.client.on_message_received = self.callback_bridge.on_message_received
        
        # Error callback
        self.client.on_error = self.callback_bridge.on_error
    
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
        """Update visible query parameters based on selected query type"""
        query_type = self.query_widget.query_type_combo.currentData()
        
        # Add help text based on query type
        help_texts = {
            QUERY_AGE_DISTRIBUTION: "Shows the distribution of ages among arrested individuals. No additional parameters needed.",
            QUERY_TOP_CHARGE_GROUPS: "Shows the most frequent charge groups. You can specify how many top groups to show.",
            QUERY_ARRESTS_BY_AREA: "Shows arrests by geographic area. You can specify how many top areas to show.",
            QUERY_ARRESTS_BY_TIME: "Shows arrests by time of day. No additional parameters needed.",
            QUERY_ARRESTS_BY_MONTH: "Shows arrests by month. You can filter by specific year or view all years.",
            QUERY_CHARGE_TYPES_BY_AREA: "Shows different charge types by geographic area. You can specify how many top areas and charge types to include.",
            QUERY_ARRESTS_BY_GENDER: "Shows arrest statistics by gender. You can filter by specific gender.",
            QUERY_ARRESTS_BY_AGE_RANGE: "Shows arrests for a specific age range. Specify minimum and maximum age.",
            QUERY_ARRESTS_BY_WEEKDAY: "Shows arrests by day of the week. No additional parameters needed.",
            QUERY_CORRELATION_ANALYSIS: "Shows correlation between different features. No additional parameters needed."
        }
        
        # Set help text
        self.query_widget.query_help_text.setText(help_texts.get(query_type, "No help available for this query type."))
        
        # Hide all parameter widgets initially
        for i in range(self.query_widget.parameters_layout.rowCount()):
            label_item = self.query_widget.parameters_layout.itemAt(i, QFormLayout.LabelRole)
            field_item = self.query_widget.parameters_layout.itemAt(i, QFormLayout.FieldRole)
            
            if label_item and field_item:
                label_widget = label_item.widget()
                field_widget = field_item.widget()
                
                if label_widget and field_widget:
                    label_widget.setVisible(False)
                    field_widget.setVisible(False)
        
        # Show relevant parameter widgets based on query type
        if query_type in [QUERY_TOP_CHARGE_GROUPS, QUERY_ARRESTS_BY_AREA, QUERY_CHARGE_TYPES_BY_AREA]:
            # Show N parameter
            n_label = self.query_widget.parameters_layout.itemAt(0, QFormLayout.LabelRole).widget()
            n_field = self.query_widget.parameters_layout.itemAt(0, QFormLayout.FieldRole).widget()
            if n_label and n_field:
                # Update label text based on query type
                if query_type == QUERY_CHARGE_TYPES_BY_AREA:
                    n_label.setText("Top N Areas/Charges:")
                else:
                    n_label.setText("Top N:")
                n_label.setVisible(True)
                n_field.setVisible(True)
        
        elif query_type == QUERY_ARRESTS_BY_MONTH:
            # Show year parameter
            year_label = self.query_widget.parameters_layout.itemAt(1, QFormLayout.LabelRole).widget()
            year_field = self.query_widget.parameters_layout.itemAt(1, QFormLayout.FieldRole).widget()
            if year_label and year_field:
                year_label.setVisible(True)
                year_field.setVisible(True)
        
        elif query_type == QUERY_ARRESTS_BY_GENDER:
            # Show gender parameter
            gender_label = self.query_widget.parameters_layout.itemAt(2, QFormLayout.LabelRole).widget()
            gender_field = self.query_widget.parameters_layout.itemAt(2, QFormLayout.FieldRole).widget()
            if gender_label and gender_field:
                gender_label.setVisible(True)
                gender_field.setVisible(True)
        
        elif query_type == QUERY_ARRESTS_BY_AGE_RANGE:
            # Show age range parameters
            min_age_label = self.query_widget.parameters_layout.itemAt(3, QFormLayout.LabelRole).widget()
            min_age_field = self.query_widget.parameters_layout.itemAt(3, QFormLayout.FieldRole).widget()
            max_age_label = self.query_widget.parameters_layout.itemAt(4, QFormLayout.LabelRole).widget()
            max_age_field = self.query_widget.parameters_layout.itemAt(4, QFormLayout.FieldRole).widget()
            
            if min_age_label and min_age_field and max_age_label and max_age_field:
                min_age_label.setVisible(True)
                min_age_field.setVisible(True)
                max_age_label.setVisible(True)
                max_age_field.setVisible(True)
    
    def toggle_connection(self):
        """Toggle connection to the server"""
        if self.client.connected:
            # Disconnect from server
            self.client.disconnect()
        else:
            # Connect to server
            host = self.host_edit.text()
            port = self.port_spinbox.value()
            
            # Update client host and port
            self.client.host = host
            self.client.port = port
            
            # Connect to server
            self.client.connect()
    
    def on_connection_status_change(self, connected):
        """Called when connection status changes"""
        self.update_ui_state()
    
    def on_login_status_change(self, logged_in):
        """Called when login status changes"""
        self.update_ui_state()
    
    def on_query_result(self, result):
        """Called when query result is received"""
        # Set title
        self.query_widget.results_title.setText(result.get('title', 'Query Results'))
        
        # Update table if data is present
        if 'data' in result and result['data'] is not None:
            df = result['data']
            
            # Set up table
            self.query_widget.results_table.clear()
            self.query_widget.results_table.setRowCount(len(df))
            self.query_widget.results_table.setColumnCount(len(df.columns))
            
            # Set headers
            self.query_widget.results_table.setHorizontalHeaderLabels(df.columns)
            
            # Fill data
            for i, row in enumerate(df.itertuples(index=False)):
                for j, value in enumerate(row):
                    item = QTableWidgetItem(str(value))
                    self.query_widget.results_table.setItem(i, j, item)
            
            # Resize columns to contents
            self.query_widget.results_table.resizeColumnsToContents()
        else:
            # Clear table
            self.query_widget.results_table.clear()
            self.query_widget.results_table.setRowCount(0)
            self.query_widget.results_table.setColumnCount(0)
        
        # Update figures if present
        if 'figure' in result and result['figure'] is not None:
            img = result['figure']
            pixmap = QPixmap.fromImage(ImageQt.ImageQt(img))
            self.query_widget.figure1_label.setPixmap(pixmap)
            self.query_widget.figure1_label.setText("")
            self.query_widget.figure1_label.setTitle(result.get('title', 'Figure 1'))
        else:
            self.query_widget.figure1_label.setPixmap(QPixmap())
            self.query_widget.figure1_label.setText("No figure available")
        
        if 'figure2' in result and result['figure2'] is not None:
            img = result['figure2']
            pixmap = QPixmap.fromImage(ImageQt.ImageQt(img))
            self.query_widget.figure2_label.setPixmap(pixmap)
            self.query_widget.figure2_label.setText("")
            self.query_widget.figure2_label.setTitle(result.get('title', 'Figure 2') + " (Detail)")
        else:
            self.query_widget.figure2_label.setPixmap(QPixmap())
            self.query_widget.figure2_label.setText("No figure available")
    
    def setup_tab_notification(self, tab_widget, tab_index, original_text):
        """Set up a notification on a tab with proper connection handling"""
        # Change the tab text with a more noticeable indicator
        tab_widget.setTabText(tab_index, "🔔 " + original_text)
        tab_widget.tabBar().setTabTextColor(tab_index, Qt.red)
        
        # Create a handler function
        def reset_tab_notification():
            if tab_widget.currentWidget() == tab_widget.widget(tab_index):
                tab_widget.setTabText(tab_index, original_text)
                
                # Use appropriate text color based on current theme
                if self.dark_theme:
                    tab_widget.tabBar().setTabTextColor(tab_index, Qt.white)
                else:
                    tab_widget.tabBar().setTabTextColor(tab_index, Qt.black)
                    
                # Remove the handler from our dictionary once used
                key = (tab_widget, tab_index)
                if key in self.tab_reset_handlers:
                    del self.tab_reset_handlers[key]
                    # Disconnect once we've handled the reset
                    try:
                        tab_widget.currentChanged.disconnect(reset_tab_notification)
                    except:
                        pass
        
        # Store the handler (overwriting any existing one)
        key = (tab_widget, tab_index)
        if key in self.tab_reset_handlers:
            # Disconnect the previous handler
            try:
                tab_widget.currentChanged.disconnect(self.tab_reset_handlers[key])
            except:
                pass
        
        # Store and connect the new handler
        self.tab_reset_handlers[key] = reset_tab_notification
        tab_widget.currentChanged.connect(reset_tab_notification)
    
    def on_message_received(self, timestamp, message):
        """Called when message is received from server"""
        # Add message to message widget
        self.message_widget.messages_text.append(f"<span style='color:blue'>[SERVER {timestamp}]</span> {message}")
        
        # Also add to the dedicated server messages area with highlighting
        self.message_widget.server_messages_text.append(f"<b style='font-size:14px'>[{timestamp}]</b>: <span style='color:darkblue;font-weight:bold'>{message}</span>")
        
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
                self.message_widget.server_messages_text.append(f"<b style='font-size:14px'>[{formatted_time}]</b>: <span style='color:darkblue;font-weight:bold'>{message['message']}</span>")
                
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
        name = self.register_widget.name_edit.text()
        nickname = self.register_widget.nickname_edit.text()
        email = self.register_widget.email_edit.text()
        password = self.register_widget.password_edit.text()
        confirm_password = self.register_widget.confirm_password_edit.text()
        
        # Validate inputs
        if not name or not nickname or not email or not password:
            QMessageBox.warning(self, "Register", "All fields are required")
            return
        
        if password != confirm_password:
            QMessageBox.warning(self, "Register", "Passwords do not match")
            return
        
        # Send registration request
        self.client.register(name, nickname, email, password)
        
        # Show login tab (registration response will be handled by callbacks)
        self.show_login()
    
    def send_query(self):
        """Send query to server"""
        # Get query type
        query_type = self.query_widget.query_type_combo.currentData()
        
        # Prepare parameters based on query type
        params = {}
        
        if query_type in [QUERY_TOP_CHARGE_GROUPS, QUERY_ARRESTS_BY_AREA]:
            params['n'] = self.query_widget.n_spinbox.value()
        elif query_type == QUERY_CHARGE_TYPES_BY_AREA:
            # Use the n_spinbox value for both n_areas and n_charges
            params['n_areas'] = self.query_widget.n_spinbox.value()
            params['n_charges'] = self.query_widget.n_spinbox.value()
        elif query_type == QUERY_ARRESTS_BY_MONTH:
            if self.query_widget.year_spinbox.value() != self.query_widget.year_spinbox.minimum():
                params['year'] = self.query_widget.year_spinbox.value()
        elif query_type == QUERY_ARRESTS_BY_GENDER:
            if self.query_widget.gender_combo.currentData() != 'all':
                params['gender'] = self.query_widget.gender_combo.currentData()
        elif query_type == QUERY_ARRESTS_BY_AGE_RANGE:
            params['min_age'] = self.query_widget.min_age_spinbox.value()
            params['max_age'] = self.query_widget.max_age_spinbox.value()
        
        # Send query
        self.client.send_query(query_type, params)
        
        # Update status bar
        self.status_bar.showMessage(f"Query sent: {QUERY_DESCRIPTIONS.get(query_type, query_type)}")
    
    def on_figure_clicked(self, pixmap, title):
        """Handle a figure being clicked"""
        # Show the plot viewer dialog
        dialog = PlotViewerDialog(pixmap, title, self)
        dialog.exec_()
    
    def closeEvent(self, event):
        """Called when window is closed"""
        # Disconnect from server
        if self.client.connected:
            self.client.disconnect()
        
        # Accept the event
        event.accept()


if __name__ == "__main__":
    # Create application
    app = QApplication(sys.argv)
    
    # Load settings to check for initial theme
    settings = QSettings("ArrestDataClient", "AppSettings")
    dark_theme = settings.value("dark_theme", True, type=bool)
    
    # Apply initial theme before creating the UI
    if dark_theme:
        app.setStyleSheet(DARK_STYLESHEET)
    else:
        app.setStyleSheet(LIGHT_STYLESHEET)
    
    # Create and show main window
    main_window = ClientGUI()
    main_window.show()
    
    # Run application
    sys.exit(app.exec()) 