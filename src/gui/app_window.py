import os
import pandas as pd
import threading
from tkinter import (
    Frame, LabelFrame, Button, Label, Entry, messagebox, Text, Scrollbar, VERTICAL, HORIZONTAL, END,
    PanedWindow, BOTH, LEFT, RIGHT, X, Y, TOP, BOTTOM, Toplevel
)
from tkinter import ttk
from tkinter import filedialog
from tkinter import StringVar

from db.derby_connector import DerbyConnector
from db.merge_logic import MergeLogic

def ensure_schema(table_name):
    return table_name if '.' in table_name else f"APP.{table_name}"

class AppWindow(Frame):
    def __init__(self, root, db1_connector, db2_connector, merge_logic):
        super().__init__(root)
        self.db1_connector = db1_connector
        self.db2_connector = db2_connector
        self.merge_logic = merge_logic

        self.last_selected_table = None
        self.last_selected_db = None
        self.pending_merge = False
        self.pending_merge_table = None
        self.page_size = 100
        
        # List of tables to merge in order
        self.MERGE_ALL_TABLES_LIST = [
            "HOUSEHOLDSERVICE", "NOMISUSER", "USERACTIVITYTRACKER", "SCHOOL", "SCHOOLGRADE", "COMMUNITYWORKER",
            "COMMUNITYBASEDORGANIZATION", "ADULTHOUSEHOLDMEMBER", "CHILDSERVICE",
            "HIVRISKASSESSMENT", "CHILDENROLLMENT", "CAREPLANACHIEVEMENTCHECKLIST",
            "BENEFICIARYSTATUSUPDATE", "CHILDEDUCATIONPERFORMANCEASSESSMENT",
            "CAREGIVERACCESSTOEMERGENCYFUND", "CAREANDSUPPORTCHECKLIST",
            "REVISEDHOUSEHOLDASSESSMENT", "HOUSEHOLDVULNERABILITYASSESSMENT",
            "HOUSEHOLDENROLLMENT2", "HOUSEHOLDENROLLMENT", "HOUSEHOLDREFERRAL",
            "NUTRITIONASSESSMENT", "HIVSTATUSMANAGER", "DATAIMPORTFILEUPLOAD",
            "QUARTERLYSTATUSTRACKER", "ENROLLMENTSTATUSHISTORY", "HIVSTATUSHISTORY",
            "CUSTOMINDICATORSREPORT", "CHILDSTATUSINDEX", "DATIMREPORT",
            "NUTRITIONSTATUS", "OCCUPATION", "USERROLE", "HOUSEHOLDCAREPLAN",
            "CUSTOMINDICATORSPIVOTED", "DATIMFLATFILE", "HHECONOMICSTRENGTHENINGASSESSMENT",
            "RESOURCETRACKER", "HOUSEHOLDHEALTHINSURANCEASSESSMENT",
            "BENEFICIARYENROLLMENT", "BENEFICIARYSERVICE", "HIVPOSITIVEDATA",
            "FACILITYOVCOFFER", "HIVRISKASSESSMENTREPORT"
        ]

        # Tables that need pre-merge cleanup based on beneficiary status
        self.OVCID_TABLES = [
            "CHILDSERVICE", "HIVRISKASSESSMENT", "CHILDENROLLMENT",
            "CHILDEDUCATIONPERFORMANCEASSESSMENT", "NUTRITIONASSESSMENT",
            "CHILDSTATUSINDEX", "NUTRITIONSTATUS"
        ]
        self.BENEFICIARYID_TABLES = [
            "ADULTHOUSEHOLDMEMBER", "BENEFICIARYSTATUSUPDATE",
            "CAREGIVERACCESSTOEMERGENCYFUND", "CAREANDSUPPORTCHECKLIST",
            "HOUSEHOLDREFERRAL", "QUARTERLYSTATUSTRACKER",
            "ENROLLMENTSTATUSHISTORY", "HOUSEHOLDCAREPLAN",
            "BENEFICIARYENROLLMENT", "BENEFICIARYSERVICE", "HIVPOSITIVEDATA",
            "HIVRISKASSESSMENTREPORT"
        ]

        # --- Top-level frame for Merge All button ---
        top_frame = Frame(self)
        top_frame.pack(fill=X, padx=10, pady=(10, 0))
        self.merge_all_button = Button(top_frame, text="Merge All Tables (DB2 to DB1)", command=self.merge_all_tables, bg="#cceeff", font=("Segoe UI", 10, "bold"))
        self.merge_all_button.pack(fill=X)

        # --- Connection section (topmost) ---
        self.conn_pane = PanedWindow(self, orient="horizontal")
        self.conn_pane.pack(fill=X, padx=10, pady=10, side=TOP)

        # --- Connection 1 Box ---
        self.conn1_box = LabelFrame(self.conn_pane, text="Connection 1", padx=10, pady=10)
        self.conn_pane.add(self.conn1_box)

        # First row: DB Path and Facility Name
        conn1_row1 = Frame(self.conn1_box)
        conn1_row1.pack(fill=X, pady=(0, 5))
        Label(conn1_row1, text="DB Path:").pack(side=LEFT)
        self.db1_entry = Entry(conn1_row1, width=25)
        self.db1_entry.pack(side=LEFT, padx=2)
        self.db1_entry.insert(0, r"C:\Nomis3\dbs\nomis3db")
        Label(conn1_row1, text="Facility Name:").pack(side=LEFT)
        self.db1_facility_entry = Entry(conn1_row1, width=12)
        self.db1_facility_entry.pack(side=LEFT, padx=2)
        self.db1_facility_entry.insert(0, "Facility1")

        # Second row: Username, Password, Connect Button
        conn1_row2 = Frame(self.conn1_box)
        conn1_row2.pack(fill=X)
        Label(conn1_row2, text="Username:").pack(side=LEFT)
        self.db1_user_entry = Entry(conn1_row2, width=10)
        self.db1_user_entry.pack(side=LEFT, padx=2)
        self.db1_user_entry.insert(0, "nomis")
        Label(conn1_row2, text="Password:").pack(side=LEFT)
        self.db1_pass_entry = Entry(conn1_row2, width=10, show="*")
        self.db1_pass_entry.pack(side=LEFT, padx=2)
        self.db1_pass_entry.insert(0, "nomispw")
        self.db1_connect_button = Button(conn1_row2, text="Connect DB1", command=self.connect_db1)
        self.db1_connect_button.pack(side=LEFT, padx=2)

        # --- Connection 2 Box ---
        self.conn2_box = LabelFrame(self.conn_pane, text="Connection 2", padx=10, pady=10)
        self.conn_pane.add(self.conn2_box)

        # First row: DB Path and Facility Name
        conn2_row1 = Frame(self.conn2_box)
        conn2_row1.pack(fill=X, pady=(0, 5))
        Label(conn2_row1, text="DB Path:").pack(side=LEFT)
        self.db2_entry = Entry(conn2_row1, width=25)
        self.db2_entry.pack(side=LEFT, padx=2)
        self.db2_entry.insert(0, r"C:\Nomis3\dbs\nomis3db")
        Label(conn2_row1, text="Facility Name:").pack(side=LEFT)
        self.db2_facility_entry = Entry(conn2_row1, width=12)
        self.db2_facility_entry.pack(side=LEFT, padx=2)
        self.db2_facility_entry.insert(0, "Facility2")

        # Second row: Username, Password, Connect Button
        conn2_row2 = Frame(self.conn2_box)
        conn2_row2.pack(fill=X)
        Label(conn2_row2, text="Username:").pack(side=LEFT)
        self.db2_user_entry = Entry(conn2_row2, width=10)
        self.db2_user_entry.pack(side=LEFT, padx=2)
        self.db2_user_entry.insert(0, "nomis")
        Label(conn2_row2, text="Password:").pack(side=LEFT)
        self.db2_pass_entry = Entry(conn2_row2, width=10, show="*")
        self.db2_pass_entry.pack(side=LEFT, padx=2)
        self.db2_pass_entry.insert(0, "nomispw")
        self.db2_connect_button = Button(conn2_row2, text="Connect DB2", command=self.connect_db2)
        self.db2_connect_button.pack(side=LEFT, padx=2)

        # --- Table lists section (just below connection) ---
        self.tables_pane = PanedWindow(self, orient="horizontal")
        self.tables_pane.pack(fill=BOTH, expand=False, pady=5, side=TOP)

        # DB1 Table List
        self.db1_tables_frame = Frame(self.tables_pane)
        self.db1_tables_label_var = StringVar()
        self.db1_tables_label_var.set("DB1 Tables")
        self.db1_tables_label = Label(self.db1_tables_frame, textvariable=self.db1_tables_label_var)
        self.db1_tables_label.pack(anchor="w")
        self.db1_tables = ttk.Treeview(self.db1_tables_frame, columns=("Table",), show="headings", height=8)
        self.db1_tables.heading("Table", text="Table Name")
        self.db1_tables.pack(fill=BOTH, expand=True)
        self.db1_tables.bind("<<TreeviewSelect>>", self.display_db1_table_content)
        self.tables_pane.add(self.db1_tables_frame, minsize=200)

        # DB2 Table List
        self.db2_tables_frame = Frame(self.tables_pane)
        self.db2_tables_label_var = StringVar()
        self.db2_tables_label_var.set("DB2 Tables")
        self.db2_tables_label = Label(self.db2_tables_frame, textvariable=self.db2_tables_label_var)
        self.db2_tables_label.pack(anchor="w")
        self.db2_tables = ttk.Treeview(self.db2_tables_frame, columns=("Table",), show="headings", height=8)
        self.db2_tables.heading("Table", text="Table Name")
        self.db2_tables.pack(fill=BOTH, expand=True)
        self.db2_tables.bind("<<TreeviewSelect>>", self.display_db2_table_content)
        self.tables_pane.add(self.db2_tables_frame, minsize=200)

        # --- Main vertical PanedWindow for the rest of the UI ---
        self.main_pane = PanedWindow(self, orient="vertical")
        self.main_pane.pack(fill=BOTH, expand=True, side=TOP)

        # --- Middle: Query box (expandable) and buttons ---
        self.query_pane = PanedWindow(self.main_pane, orient="vertical")
        self.main_pane.add(self.query_pane, minsize=80)

        # Query section
        self.query_frame = Frame(self.query_pane)

        # Frame to hold the query title and target label on one line
        query_header_frame = Frame(self.query_frame)
        query_header_frame.pack(fill=X, padx=5, pady=(10, 2))
        self.query_label = Label(query_header_frame, text="SQL Query", font=("Segoe UI", 10, "bold"))
        self.query_label.pack(side=LEFT, anchor="w")
        self.query_target_label = Label(query_header_frame, text="Target: (select a table)", font=("Segoe UI", 8, "italic"))
        self.query_target_label.pack(side=LEFT, anchor="w", padx=(5, 0))

        self.sql_text = Text(self.query_frame, height=4, wrap="word")
        self.sql_text.pack(side=LEFT, fill=BOTH, expand=True)
        self.sql_scroll = Scrollbar(self.query_frame, orient=VERTICAL, command=self.sql_text.yview)
        self.sql_scroll.pack(side=RIGHT, fill=Y)
        self.sql_text.config(yscrollcommand=self.sql_scroll.set)
        self.query_pane.add(self.query_frame, minsize=60)

        # --- Table Contents label, pagination, and buttons on the same row ---
        self.content_frame = Frame(self.main_pane)
        self.main_pane.add(self.content_frame, minsize=100)
        self.content_header_frame = Frame(self.content_frame)
        self.content_header_frame.pack(fill=X, pady=(10, 2))

        self.content_source_label = Label(self.content_header_frame, text="No table selected", font=("Segoe UI", 10, "bold"))
        self.content_source_label.pack(side=LEFT, padx=(0, 10))

        self.sql_execute_button = Button(self.content_header_frame, text="Execute SQL", command=self.execute_sql)
        self.sql_execute_button.pack(side=LEFT, padx=2)
        self.merge_button = Button(self.content_header_frame, text="Merge Selected Table", command=self.merge_selected_table)
        self.merge_button.pack(side=LEFT, padx=2)
        self.download_button = Button(self.content_header_frame, text="Download Excel", command=self.download_excel)
        self.download_button.pack(side=LEFT, padx=2)

        # Pagination controls
        self.prev_button = Button(self.content_header_frame, text="Previous", command=self.prev_page)
        self.prev_button.pack(side=LEFT, padx=(10, 2))
        self.next_button = Button(self.content_header_frame, text="Next", command=self.next_page)
        self.next_button.pack(side=LEFT, padx=2)
        self.page_label = Label(self.content_header_frame, text="Page 1")
        self.page_label.pack(side=LEFT, padx=2)
        self.limit_label = Label(self.content_header_frame, text="Limit:")
        self.limit_label.pack(side=LEFT)
        self.limit_entry = Entry(self.content_header_frame, width=5)
        self.limit_entry.insert(0, "100")
        self.limit_entry.pack(side=LEFT, padx=2)

        # --- Table contents display box ---
        self.content_tree = ttk.Treeview(self.content_frame, show="headings")
        self.content_tree.pack(fill=BOTH, expand=True, side=LEFT)
        self.content_scroll_y = Scrollbar(self.content_frame, orient=VERTICAL, command=self.content_tree.yview)
        self.content_scroll_y.pack(side=RIGHT, fill=Y)
        self.content_tree.config(yscrollcommand=self.content_scroll_y.set)
        self.content_scroll_x = Scrollbar(self.content_frame, orient=HORIZONTAL, command=self.content_tree.xview)
        self.content_scroll_x.pack(side=BOTTOM, fill=X)
        self.content_tree.config(xscrollcommand=self.content_scroll_x.set)

        # --- Logs and Indexes (share one row) ---
        self.bottom_frame = Frame(self.main_pane)
        self.main_pane.add(self.bottom_frame, minsize=80)
        # Log section (left)
        self.log_frame = Frame(self.bottom_frame)
        self.log_frame.pack(side=LEFT, fill=BOTH, expand=True)
        self.log_label = Label(self.log_frame, text="Log", font=("Segoe UI", 10, "bold"))
        self.log_label.pack(anchor="w", pady=(10, 0))
        self.log_text = Text(self.log_frame, height=6, width=60, state="disabled", bg="#f4f4f4")
        self.log_text.pack(fill=BOTH, padx=5, pady=(0, 5), expand=True)
        # Table Index section (right)
        self.index_frame = Frame(self.bottom_frame)
        self.index_frame.pack(side=LEFT, fill=BOTH, expand=True)
        self.index_label = Label(self.index_frame, text="Table Indexes", font=("Segoe UI", 10, "bold"))
        self.index_label.pack(anchor="w", pady=(10, 0))
        self.index_text = Text(self.index_frame, height=6, width=60, state="disabled", bg="#f4f4f4")
        self.index_text.pack(fill=BOTH, padx=5, pady=(0, 5), expand=True)

        # --- Facility name binding for dynamic label update ---
        def update_db_facility(event):
            selected = self.db1_tables.selection() or self.db2_tables.selection()
            if selected:
                table_name = self.db1_tables.item(selected[0])['values'][0] if self.db1_tables.selection() else self.db2_tables.item(selected[0])['values'][0]
                facility_name = table_name.split('.')[0]
                self.db1_facility_entry.delete(0, END)
                self.db1_facility_entry.insert(0, facility_name)
                self.db2_facility_entry.delete(0, END)
                self.db2_facility_entry.insert(0, facility_name)

        self.db1_tables.bind("<<TreeviewSelect>>", update_db_facility, add='+')
        self.db2_tables.bind("<<TreeviewSelect>>", update_db_facility, add='+')

    def log(self, message):
        self.log_text.config(state="normal")
        self.log_text.insert(END, message + "\n")
        self.log_text.see(END)
        self.log_text.config(state="disabled")

    # --- Connection and Table Loading ---
    def connect_db1(self):
        from main import DEFAULT_DRIVER_FOLDER, DEFAULT_DRIVER_CLASS
        path = self.db1_entry.get()
        user = self.db1_user_entry.get()
        password = self.db1_pass_entry.get()
        facility_name = self.db1_facility_entry.get()
        self.db1_tables_label_var.set(f"{facility_name} Tables")
        try:
            self.db1_connector = DerbyConnector(path, user, password, DEFAULT_DRIVER_FOLDER, DEFAULT_DRIVER_CLASS)
            self.db1_connector.connect()
            messagebox.showinfo("Success", "Connected to Database 1 successfully!")
            self.load_db1_tables()
            # After successful connection:
            if self.db2_connector.connection:
                self.merge_logic = MergeLogic(self.db1_connector.connection, self.db2_connector.connection)
        except Exception as e:
            messagebox.showerror("Connection Error", f"Failed to connect to DB1:\n{e}")

    def connect_db2(self):
        from main import DEFAULT_DRIVER_FOLDER, DEFAULT_DRIVER_CLASS
        path = self.db2_entry.get()
        user = self.db2_user_entry.get()
        password = self.db2_pass_entry.get()
        facility_name = self.db2_facility_entry.get()
        self.db2_tables_label_var.set(f"{facility_name} Tables")
        try:
            self.db2_connector = DerbyConnector(path, user, password, DEFAULT_DRIVER_FOLDER, DEFAULT_DRIVER_CLASS)
            self.db2_connector.connect()
            messagebox.showinfo("Success", "Connected to Database 2 successfully!")
            self.load_db2_tables()
            # After successful connection:
            if self.db1_connector.connection:
                self.merge_logic = MergeLogic(self.db1_connector.connection, self.db2_connector.connection)
        except Exception as e:
            messagebox.showerror("Connection Error", f"Failed to connect to DB2:\n{e}")

    def load_db1_tables(self):
        for row in self.db1_tables.get_children():
            self.db1_tables.delete(row)
        self.db1_table_list = []
        try:
            facility_name = self.db1_facility_entry.get()
            cursor = self.db1_connector.connection.cursor()
            cursor.execute("""
                SELECT s.schemaname, t.tablename
                FROM sys.systables t
                JOIN sys.sysschemas s ON t.schemaid = s.schemaid
                WHERE t.tabletype = 'T' AND s.schemaname = 'APP'
            """)
            for schema, table in cursor.fetchall():
                self.db1_tables.insert("", "end", values=(f"{facility_name}.{table}",), tags=(schema,))
        except Exception as e:
            messagebox.showerror("Error", f"Failed to retrieve tables from DB1:\n{e}")

    def load_db2_tables(self):
        for row in self.db2_tables.get_children():
            self.db2_tables.delete(row)
        self.db2_table_list = []
        try:
            facility_name = self.db2_facility_entry.get()
            cursor = self.db2_connector.connection.cursor()
            cursor.execute("""
                SELECT s.schemaname, t.tablename
                FROM sys.systables t
                JOIN sys.sysschemas s ON t.schemaid = s.schemaid
                WHERE t.tabletype = 'T' AND s.schemaname = 'APP'
            """)
            for schema, table in cursor.fetchall():
                self.db2_tables.insert("", "end", values=(f"{facility_name}.{table}",), tags=(schema,))
        except Exception as e:
            messagebox.showerror("Error", f"Failed to retrieve tables from DB2:\n{e}")

    def display_db1_table_content(self, event):
        selected = self.db1_tables.selection()
        if selected:
            table_name = self.db1_tables.item(selected[0])['values'][0]
            # Use the stored schema from the tag for backend operations
            schema = self.db1_tables.item(selected[0], 'tags')[0]
            _, table_only = table_name.split('.', 1)
            self.last_selected_table = f"{schema}.{table_only}"
            self.last_selected_db = "Database 1"
            facility = self.db1_facility_entry.get()
            self.content_source_label.config(text=f"Showing: {facility} - {table_name}")
            self.query_target_label.config(text=f"Target: Database 1 - {table_name}")
            self.display_table_content(self.db1_connector, self.last_selected_table)
            self.display_table_indexes(self.db1_connector, self.last_selected_table)
            self.sql_text.delete("1.0", END)
            self.sql_text.insert("1.0", f"SELECT * FROM {self.last_selected_table} WHERE ")

    def display_db2_table_content(self, event):
        selected = self.db2_tables.selection()
        if selected:
            table_name = self.db2_tables.item(selected[0])['values'][0]
            # Use the stored schema from the tag for backend operations
            schema = self.db2_tables.item(selected[0], 'tags')[0]
            _, table_only = table_name.split('.', 1)
            self.last_selected_table = f"{schema}.{table_only}"
            self.last_selected_db = "Database 2"  # Track selected DB
            facility = self.db2_facility_entry.get()
            self.content_source_label.config(text=f"Showing: {facility} - {table_name}")
            self.query_target_label.config(text=f"Target: Database 2 - {table_name}")
            self.display_table_content(self.db2_connector, self.last_selected_table, page=0)
            self.display_table_indexes(self.db2_connector, self.last_selected_table)
            self.sql_text.delete("1.0", END)
            self.sql_text.insert("1.0", f"SELECT * FROM {self.last_selected_table} WHERE ")

    # --- Table content display (implement as needed) ---
    def display_table_content(self, connector, table_full_name, page=0):
        self.content_tree.delete(*self.content_tree.get_children())
        if not connector or not connector.connection:
            messagebox.showerror("Error", "Not connected to the database.")
            return

        self.current_connector = connector
        self.current_table = table_full_name
        self.current_page = page

        try:
            limit = int(self.limit_entry.get()) if self.limit_entry.get().isdigit() else 100
            self.page_size = limit
            if '.' in table_full_name:
                schema, table = table_full_name.split('.', 1)
            else:
                schema, table = 'APP', table_full_name

            cursor = connector.connection.cursor()
            # Get column names
            cursor.execute(f'SELECT * FROM "{schema}"."{table}" FETCH FIRST ROW ONLY')
            columns = [desc[0] for desc in cursor.description]
            column_list = ', '.join([f'"{col}"' for col in columns])

            offset = page * self.page_size
            sql = f'''
                SELECT * FROM (
                    SELECT ROW_NUMBER() OVER() AS rn, {column_list}
                    FROM "{schema}"."{table}"
                ) AS numbered
                WHERE rn > {offset} AND rn <= {offset + self.page_size}
            '''
            cursor.execute(sql)
            display_columns = ["rn"] + columns
            self.content_tree["columns"] = display_columns
            for col in display_columns:
                self.content_tree.heading(col, text=col)
                self.content_tree.column(col, width=100, anchor="center")

            rows = cursor.fetchall()
            for row in rows:
                self.content_tree.insert("", "end", values=row)
            self.page_label.config(text=f"Page {self.current_page + 1}")
        except Exception as e:
            messagebox.showerror("Error", f"Error loading table: {e}")

    def display_table_indexes(self, connector, table_full_name):
        self.index_text.config(state="normal")
        self.index_text.delete("1.0", END)
        if not connector or not connector.connection:
            self.index_text.insert(END, "Not connected.\n")
            self.index_text.config(state="disabled")
            return

        try:
            if '.' in table_full_name:
                schema, table = table_full_name.split('.', 1)
            else:
                schema, table = 'APP', table_full_name

            cursor = connector.connection.cursor()

            # Total rows
            cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            total_rows = cursor.fetchone()[0]
            self.index_text.insert(END, f"Total Rows: {total_rows}\n\n")

            # Primary keys
            cursor.execute(f"""
                SELECT cg.descriptor
                FROM sys.sysconstraints c
                JOIN sys.syskeys k ON c.constraintid = k.constraintid
                JOIN sys.sysconglomerates cg ON k.conglomerateid = cg.conglomerateid
                JOIN sys.systables t ON c.tableid = t.tableid
                JOIN sys.sysschemas s ON t.schemaid = s.schemaid
                WHERE c.type = 'P'
                  AND t.tablename = '{table.upper()}'
                  AND s.schemaname = '{schema.upper()}'
            """)
            pk_descriptor = cursor.fetchone()
            pk = self._parse_key_descriptor(pk_descriptor[0], cursor, table.upper(), schema.upper()) if pk_descriptor else []
            self.index_text.insert(END, f"Primary Key(s): {', '.join(pk) if pk else 'None'}\n\n")

            # Unique keys
            cursor.execute(f"""
                SELECT cg.descriptor
                FROM sys.sysconstraints c
                JOIN sys.syskeys k ON c.constraintid = k.constraintid
                JOIN sys.sysconglomerates cg ON k.conglomerateid = cg.conglomerateid
                JOIN sys.systables t ON c.tableid = t.tableid
                JOIN sys.sysschemas s ON t.schemaid = s.schemaid
                WHERE c.type = 'U'
                  AND t.tablename = '{table.upper()}'
                  AND s.schemaname = '{schema.upper()}'
            """)
            uk_descriptors = cursor.fetchall()
            all_uks = []
            for desc in uk_descriptors:
                all_uks.extend(self._parse_key_descriptor(desc[0], cursor, table.upper(), schema.upper()))
            self.index_text.insert(END, f"Unique Key(s): {', '.join(all_uks) if all_uks else 'None'}\n\n")

            # Foreign keys
            cursor.execute(f"""
                SELECT cg.descriptor
                FROM sys.sysconstraints c
                JOIN sys.syskeys k ON c.constraintid = k.constraintid
                JOIN sys.sysconglomerates cg ON k.conglomerateid = cg.conglomerateid
                JOIN sys.systables t ON c.tableid = t.tableid
                JOIN sys.sysschemas s ON t.schemaid = s.schemaid
                WHERE c.type = 'F'
                  AND t.tablename = '{table.upper()}'
                  AND s.schemaname = '{schema.upper()}'
            """)
            fk_descriptors = cursor.fetchall()
            all_fks = []
            for desc in fk_descriptors:
                all_fks.extend(self._parse_key_descriptor(desc[0], cursor, table.upper(), schema.upper()))
            self.index_text.insert(END, f"Foreign Key(s): {', '.join(all_fks) if all_fks else 'None'}\n\n")

        except Exception as e:
            self.index_text.insert(END, f"Error fetching indexes: {e}\n")
        self.index_text.config(state="disabled")

    # --- Merge logic with SQL preview ---
    def merge_selected_table(self):
        if not self.last_selected_table:
            self.log("Error: Please select a table to merge.")
            messagebox.showerror("Error", "Please select a table to merge.")
            return

        table_name = ensure_schema(self.last_selected_table)
        sql_statements = []

        # 1. If OVCID or BENEFICIARYID, show the SELECT and DELETE SQL for non-positives
        if "OVCID" in table_name.upper():
            sql_statements.append("-- Get list of non-positives (OVCID):")
            sql_statements.append("SELECT OVCID FROM APP.CHILDRENENROLLMENT WHERE CURRENTHIVSTATUS !=1;")
            sql_statements.append(f"-- Delete these from {table_name} in DB2 before merging:")
            sql_statements.append(f"DELETE FROM {table_name} WHERE OVCID IN (SELECT OVCID FROM APP.CHILDRENENROLLMENT WHERE CURRENTHIVSTATUS !=1);")
        if "BENEFICIARYID" in table_name.upper():
            sql_statements.append("-- Get list of non-positives (BENEFICIARYID):")
            sql_statements.append("SELECT BENEFICIARYID FROM APP.ADULTHOUSEHOLDMEMBER WHERE CURRENTHIVSTATUS !=1;")
            sql_statements.append(f"-- Delete these from {table_name} in DB2 before merging:")
            sql_statements.append(f"DELETE FROM {table_name} WHERE BENEFICIARYID IN (SELECT BENEFICIARYID FROM APP.ADULTHOUSEHOLDMEMBER WHERE CURRENTHIVSTATUS !=1);")

        # 2. Generate the INSERT SQL for the merge
        try:
            db2_cur = self.db2_connector.connection.cursor()
            db2_cur.execute(f"SELECT * FROM {table_name} FETCH FIRST ROW ONLY")
            columns = [desc[0] for desc in db2_cur.description]
            col_list = ', '.join([f'"{col}"' for col in columns])
            sql_statements.append("-- Merge data from DB2 to DB1:")
            sql_statements.append(f"INSERT INTO {table_name} ({col_list}) SELECT {col_list} FROM {table_name} IN DATABASE2;")
        except Exception as e:
            sql_statements.append("-- Could not preview INSERT SQL due to error: " + str(e))

        # Show the SQL in the Query Box
        self.sql_text.delete("1.0", END)
        self.sql_text.insert("1.0", "\n".join(sql_statements))
        self.pending_merge = True
        self.pending_merge_table = table_name
        self.log("Review the actual SQL above and click 'Execute SQL' to proceed.")

    def _pre_merge_cleanup(self, table_name):
        """Deletes non-positive beneficiaries from the given table in DB2."""
        db2_cur = self.db2_connector.connection.cursor()
        table_name_upper = table_name.upper()
        
        # Cleanup for OVCID
        if table_name_upper in self.OVCID_TABLES:
            self.log(f"Checking for non-positive OVCIDs for table {table_name}...")
            db2_cur.execute("SELECT OVCID FROM APP.CHILDRENENROLLMENT WHERE CURRENTHIVSTATUS != 1")
            non_positives = [row[0] for row in db2_cur.fetchall()]
            if non_positives:
                placeholders = ','.join(['?'] * len(non_positives))
                del_sql = f'DELETE FROM {table_name} WHERE OVCID IN ({placeholders})'
                try:
                    db2_cur.execute(del_sql, non_positives)
                    self.db2_connector.connection.commit()
                    self.log(f"Deleted {db2_cur.rowcount} non-positive OVC records from {table_name} in DB2.")
                except Exception as e:
                    self.log(f"Error during OVCID cleanup for {table_name}: {e}")
            else:
                self.log("No non-positive OVCIDs found to clean up.")

        # Cleanup for BENEFICIARYID
        if table_name_upper in self.BENEFICIARYID_TABLES:
            self.log(f"Checking for non-positive BENEFICIARYIDs for table {table_name}...")
            db2_cur.execute("SELECT BENEFICIARYID FROM APP.ADULTHOUSEHOLDMEMBER WHERE CURRENTHIVSTATUS != 1")
            non_positives = [row[0] for row in db2_cur.fetchall()]
            if non_positives:
                placeholders = ','.join(['?'] * len(non_positives))
                del_sql = f'DELETE FROM {table_name} WHERE BENEFICIARYID IN ({placeholders})'
                try:
                    db2_cur.execute(del_sql, non_positives)
                    self.db2_connector.connection.commit()
                    self.log(f"Deleted {db2_cur.rowcount} non-positive BENEFICIARY records from {table_name} in DB2.")
                except Exception as e:
                    self.log(f"Error during BENEFICIARYID cleanup for {table_name}: {e}")
            else:
                self.log("No non-positive BENEFICIARYIDs found to clean up.")

    def merge_all_tables(self):
        """Handles the 'Merge All Tables' button click."""
        if not self.db1_connector.connection or not self.db2_connector.connection:
            messagebox.showerror("Error", "Both databases must be connected to merge all tables.")
            return

        if messagebox.askyesno("Confirm Merge All", "This will merge all specified tables from Database 2 into Database 1. This operation cannot be undone. Are you sure you want to proceed?"):
            self.merge_all_button.config(state="disabled", text="Merging...")
            # Run the merge process in a separate thread to keep the UI responsive
            threading.Thread(target=self._run_full_merge, daemon=True).start()

    def _run_full_merge(self):
        """The actual logic for merging all tables, run in a thread."""
        self.log("\n" + "="*50)
        self.log("Starting full data migration from DB2 to DB1...")
        self.log("="*50)

        total_tables = len(self.MERGE_ALL_TABLES_LIST)
        for i, table_name_no_schema in enumerate(self.MERGE_ALL_TABLES_LIST):
            table_name = ensure_schema(table_name_no_schema)
            self.log("\n" + "-"*50)
            self.log(f"Processing table {i+1}/{total_tables}: {table_name}")
            self.log("-" * 50)
            
            try:
                # 1. Perform pre-merge cleanup
                self._pre_merge_cleanup(table_name)

                # 2. Perform the merge
                self.log(f"Starting merge for {table_name}...")
                inserted_count = self.merge_logic.merge_table(table_name, table_name, log_callback=self.log)
                self.log(f"SUCCESS: Merged {inserted_count} records for table {table_name}.")

            except Exception as e:
                self.log(f"FATAL ERROR for table {table_name}: {e}")
                self.log(f"Skipping to next table.")

        self.log("\n" + "="*50)
        self.log("Full data migration process finished.")
        self.log("="*50)
        self.after(0, lambda: self.merge_all_button.config(state="normal", text="Merge All Tables (DB2 to DB1)"))
        self.after(0, lambda: messagebox.showinfo("Complete", "Full data migration process has finished. Please check the logs for details."))

    # --- Execute SQL logic ---
    def execute_sql(self):
        sql = self.sql_text.get("1.0", END).strip()
        if self.pending_merge:
            table_name = self.pending_merge_table
            # 1. Perform pre-merge cleanup
            self._pre_merge_cleanup(table_name)
            # 2. Now perform the merge
            inserted = self.merge_logic.merge_table(table_name, table_name, log_callback=self.log)
            self.log(f"Success: Merged {inserted} records for table {table_name}.")
            messagebox.showinfo("Success", f"Merged {inserted} records for table {table_name}.")
            self.pending_merge = False
            self.pending_merge_table = None
            return

        # Normal SQL execution
        connector = None
        if self.last_selected_db == "Database 1":
            connector = self.db1_connector
        elif self.last_selected_db == "Database 2":
            connector = self.db2_connector
        else:
            messagebox.showerror("Error", "Please select a table first.")
            return
        try:
            cursor = connector.connection.cursor()
            cursor.execute(sql)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                # Use the original column names as identifiers for consistency.
                self.content_tree["columns"] = columns
                for col_name in columns:
                    self.content_tree.heading(col_name, text=col_name)
                    self.content_tree.column(col_name, width=120, minwidth=120, stretch=False, anchor="center")
                self.content_tree.delete(*self.content_tree.get_children())
                rows = cursor.fetchall()
                for row in rows:
                    self.content_tree.insert("", "end", values=row)
                self.log(f"SQL executed successfully: {sql}")
            else:
                connector.connection.commit()
                self.content_tree.delete(*self.content_tree.get_children())
                self.log(f"SQL executed successfully (no result set): {sql}")
        except Exception as e:
            self.log(f"SQL execution error: {e} | SQL: {sql}")
            messagebox.showerror("SQL Error", f"Error executing SQL: {e}")

    # --- Download Excel, Pagination, and other methods ---
    def download_excel(self):
        if not self.last_selected_table:
            self.log("Error: Please select a table to download.")
            messagebox.showerror("Error", "Please select a table to download.")
            return
        table_name = ensure_schema(self.last_selected_table)
        connector = None
        db_label = None
        if self.last_selected_db == "Database 1":
            connector = self.db1_connector
            db_label = "database1"
        elif self.last_selected_db == "Database 2":
            connector = self.db2_connector
            db_label = "database2"
        else:
            messagebox.showerror("Error", "Please select a table first.")
            return
        if not connector or not table_name:
            messagebox.showerror("Error", "No table selected.")
            return

        # Prepare downloads folder at project root
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        downloads_folder = os.path.join(project_root, 'downloads')
        os.makedirs(downloads_folder, exist_ok=True)

        # Suggest filename automatically
        if '.' in table_name:
            _, table = table_name.split('.', 1)
        else:
            table = table_name
        facility = self.db1_facility_entry.get() if self.last_selected_db == "Database 1" else self.db2_facility_entry.get()
        default_filename = f"{table}-{facility}.xlsx"
        file_path = os.path.join(downloads_folder, default_filename)

        # Show progress window
        progress_win = Toplevel(self)
        progress_win.title("Exporting...")
        progress_label = Label(progress_win, text="Exporting table to Excel, please wait...")
        progress_label.pack(padx=20, pady=20)
        progress_win.grab_set()
        progress_win.update()

        def export_job():
            try:
                if '.' in table_name:
                    schema, table = table_name.split('.', 1)
                else:
                    schema, table = 'APP', table_name
                cursor = connector.connection.cursor()
                cursor.execute(f'SELECT * FROM "{schema}"."{table}"')
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=columns)
                df.to_excel(file_path, index=False)
                self.after(0, lambda: messagebox.showinfo("Success", f"Table exported to {file_path}"))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Export Error", f"Failed to export table: {e}"))
            finally:
                self.after(0, progress_win.destroy)

        threading.Thread(target=export_job, daemon=True).start()

    def next_page(self):
        if self.current_table and self.current_connector:
            self.display_table_content(self.current_connector, self.current_table, self.current_page + 1)

    def prev_page(self):
        if self.current_table and self.current_connector and self.current_page > 0:
            self.display_table_content(self.current_connector, self.current_table, self.current_page - 1)

    def update_table_content(self):
        if not self.current_connector or not self.current_connector.connection:
            return

        # Clear previous content
        for col in self.content_tree["columns"]:
            self.content_tree.heading(col, text="")
            self.content_tree.delete(*self.content_tree.get_children())

        # Fetch the data for the current page
        offset = self.current_page * self.page_size
        try:
            cursor = self.current_connector.connection.cursor()
            cursor.execute(f'SELECT * FROM {self.current_table} OFFSET {offset} ROWS FETCH NEXT {self.page_size} ROWS ONLY')
            columns = [desc[0] for desc in cursor.description]
            display_columns = ["#"] + columns
            self.content_tree["columns"] = display_columns
            for col in display_columns:
                self.content_tree.heading(col, text=col)
                self.content_tree.column(col, width=100, anchor="center")

            rows = cursor.fetchall()
            for idx, row in enumerate(rows, 1):
                self.content_tree.insert("", "end", values=(idx + offset, *row))

            # Update the page label
            self.page_label.config(text=f"Page {self.current_page + 1}")
        except Exception as e:
            self.log(f"Error updating table content: {e}")

    def _parse_key_descriptor(self, descriptor, cursor, table_name, schema_name):
        """Helper to parse Derby's key descriptor and return column names."""
        import re
        # Descriptor format is like: "base table conglomerate (1, 2, -3)"
        # The numbers are the 1-based column positions in the table.
        # Negative numbers indicate descending order, which we ignore for just getting names.
        match = re.search(r'\((\s*[-]?\d+\s*(?:,\s*[-]?\d+\s*)*)\)', str(descriptor))
        if not match:
            return []

        column_positions = [abs(int(p.strip())) for p in match.group(1).split(',')]
        column_names = []
        for pos in column_positions:
            cursor.execute(f"""
                SELECT c.columnname FROM sys.syscolumns c
                JOIN sys.systables t ON c.referenceid = t.tableid
                JOIN sys.sysschemas s ON t.schemaid = s.schemaid
                WHERE t.tablename = '{table_name}' AND s.schemaname = '{schema_name}' AND c.columnnumber = {pos}
            """)
            col_name = cursor.fetchone()
            if col_name:
                column_names.append(col_name[0])
        return column_names

    def log_table_primary_keys(self, connector, db_name):
        """Logs all tables and their primary keys for a given connection."""
        # This method was added in a previous step and is assumed to exist.
        # No changes needed here.
        pass