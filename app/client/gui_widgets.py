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

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg

from datetime import datetime
import logging
import os
import tempfile

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

class LoginWidget(QWidget):
    """Login widget for the client GUI"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        self.setup_ui()
    
    def setup_ui(self):
        """Set up the UI for the login widget"""
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
        
        self.setup_ui()
    
    def setup_ui(self):
        """Set up the UI for the register widget"""

        layout = QVBoxLayout(self)
        
        form_layout = QFormLayout()
        
        self.name_edit = QLineEdit()
        form_layout.addRow("Name:", self.name_edit)
        
        self.nickname_edit = QLineEdit()
        form_layout.addRow("Nickname:", self.nickname_edit)
        
        self.email_edit = QLineEdit()
        form_layout.addRow("Email:", self.email_edit)
        
        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.Password)
        form_layout.addRow("Password:", self.password_edit)
        
        self.confirm_password_edit = QLineEdit()
        self.confirm_password_edit.setEchoMode(QLineEdit.Password)
        form_layout.addRow("Confirm Password:", self.confirm_password_edit)
        
        layout.addLayout(form_layout)
        
        layout.addSpacing(20)
        
        self.register_button = QPushButton("Register")
        layout.addWidget(self.register_button)
        
        self.back_button = QPushButton("Back to Login")
        layout.addWidget(self.back_button)
        
        layout.addStretch()


class QueryWidget(QWidget):
    """Widget for sending queries and viewing results"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()
    
    def setup_ui(self):
        """Set up the query UI components"""
        layout = QVBoxLayout(self)
        
        query_group = QGroupBox("Query Parameters")
        query_layout = QVBoxLayout()

        self.query_type_combo = QComboBox()
        self.query_type_combo.addItems([
            "1: Arrestaties per Gebied en Tijdsperiode",
            "2: Trend van Specifieke Overtreding over Tijd",
            "3: Demografische Analyse van Arrestaties (Graph)",
            "4: Geografische Hotspots van Arrestaties"
        ])
        query_layout.addWidget(QLabel("Select Query Type:"))
        query_layout.addWidget(self.query_type_combo)

        self.parameter_stack = QStackedWidget()
        self.parameter_stack.addWidget(self._create_query1_params())
        self.parameter_stack.addWidget(self._create_query2_params())
        self.parameter_stack.addWidget(self._create_query3_params())
        self.parameter_stack.addWidget(self._create_query4_params())
        query_layout.addWidget(self.parameter_stack)

        # Connect query type change to stack change
        self.query_type_combo.currentIndexChanged.connect(self.parameter_stack.setCurrentIndex)

        query_group.setLayout(query_layout)
        layout.addWidget(query_group)

        self.send_query_button = QPushButton(QIcon.fromTheme("system-search"), "Send Query")
        self.send_query_button.setFont(QFont("Arial", 10, QFont.Bold))
        layout.addWidget(self.send_query_button)
        
        results_group = QGroupBox("Query Results")
        results_layout = QVBoxLayout()
        
        # Splitter for Table and Plot
        self.results_splitter = QSplitter(Qt.Vertical)
        
        # Table for results
        self.results_table = QTableWidget()
        self.results_table.setColumnCount(5) # Example column count, adjust as needed
        self.results_table.setHorizontalHeaderLabels(["Report ID", "Date", "Area", "Charge", "Age"]) # Example headers
        self.results_table.setAlternatingRowColors(True)
        self.results_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.results_table.verticalHeader().setVisible(False)
        self.results_splitter.addWidget(self.results_table)

        # --- Wrap plot label in a Scroll Area --- 
        self.plot_scroll_area = QScrollArea()
        self.plot_scroll_area.setWidgetResizable(True) # Allow widget inside to resize
        self.plot_scroll_area.setAlignment(Qt.AlignCenter) # Center the widget if smaller
        self.plot_scroll_area.setStyleSheet("background-color: #1E1E1E;") # Match dark theme bg

        # Use a FigureCanvasQTAgg widget to display the plot
        # We need a placeholder figure initially
        from matplotlib.figure import Figure
        placeholder_fig = Figure(figsize=(5, 4), dpi=100)
        self.plot_canvas = FigureCanvasQTAgg(placeholder_fig)

        # Set the canvas as the widget for the scroll area
        self.plot_scroll_area.setWidget(self.plot_canvas)
        self.plot_scroll_area.hide() # Hide scroll area initially
        # -----------------------------------------

        # Add the scroll area (containing the canvas) to the splitter
        self.results_splitter.addWidget(self.plot_scroll_area)
        
        results_layout.addWidget(self.results_splitter)
        results_group.setLayout(results_layout)
        
        layout.addWidget(results_group, 1) 

        # Store the current figure for the viewer dialog
        self.current_figure = None
        self.plot_canvas.installEventFilter(self)

    def eventFilter(self, source, event):
        """Handle events, specifically clicks on the plot canvas."""
        # Check if the source is the plot canvas
        if source == self.plot_canvas and event.type() == QEvent.MouseButtonPress:
            if event.button() == Qt.LeftButton and self.current_figure:
                # --- ADD LOGGING ---
                logger.info("Plot canvas clicked, emitting plot_clicked signal with figure.")
                # -------------------
                # Emit signal with the stored figure object
                self.plot_clicked.emit(self.current_figure)
                return True # Event handled
        # Pass event along if not handled
        return False

    # Add signal for plot clicks
    plot_clicked = Signal(object) # Emits the matplotlib Figure object

    def _create_query1_params(self):
        """Create parameter widget for Query 1"""
        widget = QWidget()
        layout = QFormLayout(widget)
        self.q1_area_combo = QComboBox() 
        self.q1_area_combo.addItems(["Loading areas..."])
        self.q1_start_date = QDateEdit(datetime.now().date())
        self.q1_end_date = QDateEdit(datetime.now().date())
        self.q1_min_age_spin = QSpinBox()
        self.q1_max_age_spin = QSpinBox()
        self.q1_min_age_spin.setRange(0, 90)
        self.q1_max_age_spin.setRange(0, 900)
        self.q1_max_age_spin.setValue(90)

        layout.addRow("Gebied (Area Name):", self.q1_area_combo)
        layout.addRow("Startdatum:", self.q1_start_date)
        layout.addRow("Einddatum:", self.q1_end_date)
        layout.addRow("Minimum Leeftijd:", self.q1_min_age_spin)
        layout.addRow("Maximum Leeftijd:", self.q1_max_age_spin)
        return widget

    def _create_query2_params(self):
        """Create parameter widget for Query 2"""
        widget = QWidget()
        layout = QFormLayout(widget)
        self.q2_charge_combo = QComboBox()
        self.q2_charge_combo.addItems(["Loading charge types..."])
        self.q2_granularity_combo = QComboBox()
        self.q2_granularity_combo.addItems(["Daily", "Weekly", "Monthly", "Yearly"])
        # self.q2_area_input = QLineEdit() # <-- REMOVED
        # self.q2_area_input.setPlaceholderText("Optional: Enter areas separated by commas") # <-- REMOVED

        layout.addRow("Arrestatietype:", self.q2_charge_combo)
        layout.addRow("Tijdsgranulariteit:", self.q2_granularity_combo)
        # layout.addRow("Gebied(en) (optioneel):", self.q2_area_input) # <-- REMOVED
        return widget

    def _create_query3_params(self):
        """Create parameter widget for Query 3"""
        widget = QWidget()
        layout = QFormLayout(widget)
        self.q3_sex_m_check = QCheckBox("Male (M)")
        self.q3_sex_f_check = QCheckBox("Female (F)")
        sex_layout = QHBoxLayout()
        sex_layout.addWidget(self.q3_sex_m_check)
        sex_layout.addWidget(self.q3_sex_f_check)

        self.q3_descent_list = QListWidget()
        self.q3_descent_list.setMinimumHeight(100) 
        self.q3_descent_list.setMaximumHeight(200) 

        # Charge Group
        self.q3_charge_combo = QComboBox()
        self.q3_charge_combo.addItems(["Optional: Loading charge groups..."]) # Changed text for clarity

        # Arrest Type (New)
        self.q3_arrest_type_combo = QComboBox() # <<< DEFINE q3_arrest_type_combo
        self.q3_arrest_type_combo.addItems(["Optional: Loading arrest types..."]) # Changed text

        layout.addRow("Geslacht (Sex Code):", sex_layout)
        layout.addRow("Etniciteit (Descent):", self.q3_descent_list)
        layout.addRow("Charge Group (optioneel):", self.q3_charge_combo) # Corrected label and widget
        layout.addRow("Arrestatietype (optioneel):", self.q3_arrest_type_combo) # Added new row for arrest type
        return widget


    def _create_query4_params(self):
        """Create parameter widget for Query 4"""
        widget = QWidget()
        layout = QFormLayout(widget)

        # --- Add LAT/LON SpinBoxes ---
        self.q4_center_lat_spin = QDoubleSpinBox()
        self.q4_center_lat_spin.setRange(-90.0, 90.0)
        self.q4_center_lat_spin.setDecimals(6) 
        self.q4_center_lat_spin.setValue(34.0522) # Example: Default to LA center

        self.q4_center_lon_spin = QDoubleSpinBox()
        self.q4_center_lon_spin.setRange(-180.0, 180.0)
        self.q4_center_lon_spin.setDecimals(6) 
        self.q4_center_lon_spin.setValue(-118.2437) # Example: Default to LA center

        lat_lon_layout = QHBoxLayout()
        lat_lon_layout.addWidget(QLabel("Latitude:"))
        lat_lon_layout.addWidget(self.q4_center_lat_spin)
        lat_lon_layout.addSpacing(10)
        lat_lon_layout.addWidget(QLabel("Longitude:"))
        lat_lon_layout.addWidget(self.q4_center_lon_spin)
        layout.addRow("Centrumpunt (LAT/LON):", lat_lon_layout)
        # ---------------------------

        self.q4_radius_spin = QDoubleSpinBox()
        self.q4_radius_spin.setRange(0.1, 100.0)
        self.q4_radius_spin.setValue(1.0)
        self.q4_radius_spin.setSuffix(" km")
        self.q4_start_date = QDateEdit(datetime.now().date())
        self.q4_end_date = QDateEdit(datetime.now().date())

        self.q4_arrest_type_combo = QComboBox()
        self.q4_arrest_type_combo.addItems(["Loading types..."]) 

        layout.addRow("Radius:", self.q4_radius_spin)
        layout.addRow("Startdatum:", self.q4_start_date)
        layout.addRow("Einddatum:", self.q4_end_date)
        layout.addRow("Arrestatietype (optioneel):", self.q4_arrest_type_combo)
        return widget

    def display_results(self, results):
        """Display query results in the table"""
        # --- Reset splitter sizes when showing table ---
        original_sizes = self.results_splitter.sizes()
        # Default: give table more space initially if we have sizes, else split roughly 70/30
        table_height = int(self.results_splitter.height() * 0.7) if sum(original_sizes) == 0 else original_sizes[0]
        plot_height = self.results_splitter.height() - table_height
        self.results_splitter.setSizes([table_height, plot_height]) 
        # ----------------------------------------------

        self.results_table.setRowCount(0) # Clear previous results
        self.plot_scroll_area.hide() # Hide plot scroll area
        self.results_table.show() # Show table by default

        # Check if results are empty or invalid
        # If no results found, display a message in the table
        # spanning across all columns and exit the function
        if not results or 'data' not in results or not results['data']:
            self.results_table.setRowCount(1)
            no_results_item = QTableWidgetItem("No results found or empty data.")
            no_results_item.setTextAlignment(Qt.AlignCenter)
            self.results_table.setItem(0, 0, no_results_item)
            self.results_table.setSpan(0, 0, 1, self.results_table.columnCount())
            logger.info("No results to display.")
            return

        data = results['data']
        headers = results.get('headers', [])
        
        if not headers:
             # Attempt to infer headers if not provided
            if isinstance(data[0], dict):
                headers = list(data[0].keys())
            else:
                # Fallback if data format is unexpected
                headers = [f"Column {i+1}" for i in range(len(data[0]))] if data else []

        if not headers:
             logger.warning("Could not determine headers for results table.")
             self.results_table.setRowCount(1)
             item = QTableWidgetItem("Error: Could not determine result headers.")
             item.setTextAlignment(Qt.AlignCenter)
             self.results_table.setItem(0, 0, item)
             self.results_table.setSpan(0, 0, 1, 1) 
             return


        self.results_table.setColumnCount(len(headers))
        self.results_table.setHorizontalHeaderLabels(headers)
        
        self.results_table.setRowCount(len(data))
        
        for row_idx, row_data in enumerate(data):
            if isinstance(row_data, dict):
                for col_idx, header in enumerate(headers):
                    item = QTableWidgetItem(str(row_data.get(header, "")))
                    self.results_table.setItem(row_idx, col_idx, item)
            elif isinstance(row_data, (list, tuple)):
                 if len(row_data) == len(headers):
                    for col_idx, cell_data in enumerate(row_data):
                        item = QTableWidgetItem(str(cell_data))
                        self.results_table.setItem(row_idx, col_idx, item)
                 else:
                     logger.warning(f"Row {row_idx} data length mismatch: expected {len(headers)}, got {len(row_data)}")
                     item = QTableWidgetItem("Data format error")
                     self.results_table.setItem(row_idx, 0, item) # Indicate error in first cell
            else:
                # Handle unexpected row format
                logger.warning(f"Unexpected data format in row {row_idx}: {type(row_data)}")
                item = QTableWidgetItem("Unexpected data format")
                self.results_table.setItem(row_idx, 0, item)


        self.results_table.resizeColumnsToContents()
        logger.info(f"Displayed {len(data)} results.")

    def display_plot(self, fig, title="Query Result Plot"):
        """Display a matplotlib figure using FigureCanvasQTAgg"""
        if fig is None:
            logger.warning("display_plot called with None figure.")
            self.display_error_in_plot_area("Received empty plot data.")
            return
            
        try:
            # Store the figure for potential click events
            self.current_figure = fig
            
            # Create a NEW canvas with the received figure
            self.plot_canvas = FigureCanvasQTAgg(fig)
            
            # --- Prevent blurry scaling --- 
            # Calculate native size based on figure DPI
            dpi = fig.get_dpi()
            width_inches = fig.get_figwidth()
            height_inches = fig.get_figheight()
            width_pixels = int(width_inches * dpi)
            height_pixels = int(height_inches * dpi)
            # Set the canvas to its native pixel size
            self.plot_canvas.setFixedSize(width_pixels, height_pixels)
            # --------------------------- 
            
            # Set the new canvas as the widget for the scroll area
            self.plot_scroll_area.setWidget(self.plot_canvas)

            # Re-install the event filter on the NEW canvas
            self.plot_canvas.installEventFilter(self)

            self.results_table.hide()
            self.plot_scroll_area.show()
            
            # No need to resize splitter here, let scroll area handle it
            # plot_height = self.results_splitter.height()
            # table_height = 0
            # self.results_splitter.setSizes([table_height, plot_height])

            logger.info(f"Matplotlib Figure displayed (native size: {width_pixels}x{height_pixels}) using FigureCanvasQTAgg.")
            
            # TODO: Add title display? (Canvas doesn't have setTitle like QLabel)
            # Maybe add a separate QLabel above the scroll area for the title.
            
            # TODO: Implement click-to-enlarge for canvas?
            # This would require custom event handling on the canvas widget.

        except Exception as e:
            logger.error(f"Error displaying plot with FigureCanvasQTAgg: {e}", exc_info=True)
            self.display_error_in_plot_area(f"Error displaying plot: {e}")
            
    def display_error_in_plot_area(self, message):
        """Displays an error message in the plot canvas area"""
        from matplotlib.figure import Figure # Import Figure locally for error display
        # Create a simple figure with error text
        error_fig = Figure()
        ax = error_fig.add_subplot(111)
        ax.text(0.5, 0.5, message, ha='center', va='center', color='red', wrap=True)
        ax.set_xticks([])
        ax.set_yticks([])
        # Create a new canvas for the error message
        self.plot_canvas = FigureCanvasQTAgg(error_fig)
        self.plot_scroll_area.setWidget(self.plot_canvas)
        self.plot_scroll_area.show() # Show scroll area with error message
        self.results_table.hide()

    def clear_results(self):
        """Clear the results table and plot"""
        original_sizes = self.results_splitter.sizes()
        table_height = int(self.results_splitter.height() * 0.7) if sum(original_sizes) == 0 else original_sizes[0]
        plot_height = self.results_splitter.height() - table_height
        self.results_splitter.setSizes([table_height, plot_height])
        # ----------------------------------------
        self.results_table.setRowCount(0)
        self.results_table.setColumnCount(0)
        # Clear the plot by setting a blank figure
        from matplotlib.figure import Figure
        blank_fig = Figure()
        self.plot_canvas = FigureCanvasQTAgg(blank_fig)
        self.plot_scroll_area.setWidget(self.plot_canvas)
        self.plot_scroll_area.hide() # Hide scroll area
        self.results_table.show()
        # Clear the stored figure reference
        self.current_figure = None 
        logger.info("Query results cleared.")


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