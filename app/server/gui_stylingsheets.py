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

QTableWidget {
    background-color: #1E1E1E;
    alternate-background-color: #2D2D30;
    color: #E1E1E1; /* Default text color for items */
    gridline-color: #3F3F46;
    selection-background-color: #007ACC;
}

/* Style default and alternate items explicitly */
QTableView::item {
    color: #E1E1E1; /* Light text for default rows */
    background-color: transparent; /* Make background transparent to see QTableWidget's background */
}

QTableView::item:alternate {
    color: #E1E1E1; /* Light text for alternate rows */
    background-color: transparent; /* Make background transparent to see QTableWidget's alternate background */
}

QHeaderView::section {
    background-color: #3F3F46;
    color: #E1E1E1;
    padding: 4px;
    border: 1px solid #3F3F46;
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