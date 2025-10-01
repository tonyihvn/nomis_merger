import tkinter as tk
from tkinter import messagebox
from gui.app_window import AppWindow
from db.derby_connector import DerbyConnector
from db.merge_logic import MergeLogic
import os

DEFAULT_DRIVER_CLASS = "org.apache.derby.iapi.jdbc.AutoloadedDriver"
DEFAULT_DRIVER_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), "db", "DerbyJdbcDriver"))

class MainApplication:
    def __init__(self, root):
        self.root = root
        self.root.title("Derby Database Merge Tool")
        
        # Pass the folder containing all JARs
        self.db1_connector = DerbyConnector("", "", "", DEFAULT_DRIVER_FOLDER, DEFAULT_DRIVER_CLASS)
        self.db2_connector = DerbyConnector("", "", "", DEFAULT_DRIVER_FOLDER, DEFAULT_DRIVER_CLASS)
        
        self.merge_logic = MergeLogic(self.db1_connector.connection, self.db2_connector.connection)

        self.app_window = AppWindow(self.root, self.db1_connector, self.db2_connector, self.merge_logic)
        self.app_window.pack(fill=tk.BOTH, expand=True)

    def run(self):
        self.root.mainloop()

if __name__ == "__main__":
    root = tk.Tk()
    app = MainApplication(root)
    app.run()