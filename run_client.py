#!/usr/bin/env python3
# Run script for the client GUI

import os
import sys

# Add the app directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'app')))

# Import client GUI
from app.client.client_gui import ClientGUI
from PySide6.QtWidgets import QApplication

def main():
    """Main function to run the client GUI"""
    app = QApplication(sys.argv)
    window = ClientGUI()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main() 