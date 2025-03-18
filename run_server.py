#!/usr/bin/env python3
# Run script for the server GUI

import os
import sys

# Add the app directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'app')))

# Import server GUI
from app.server.server_gui import ServerGUI
from PySide6.QtWidgets import QApplication

def main():
    """Main function to run the server GUI"""
    app = QApplication(sys.argv)
    window = ServerGUI()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main() 