import os
from tkinter import Frame, Button, Label, Entry, messagebox, Text, Scrollbar, VERTICAL, HORIZONTAL, END, PanedWindow, BOTH, LEFT, RIGHT, X, Y, TOP, BOTTOM
from tkinter import ttk
from db.derby_connector import DerbyConnector
from db.merge_logic import MergeLogic

class AppWindow(Frame):
    def __init__(self, root, db1_connector, db2_connector, merge_logic):
        super().__init__(root)
        self.db1_connector = db1_connector
        self.db2_connector = db2_connector
        self.merge_logic = merge_logic

        self.page_size = 100
        self.current_page = 0
        self.current_table = None
        self.current_connector = None

        # Main PanedWindow
        self.main_pane = PanedWindow(self, orient="vertical")
        self.main_pane.pack(fill=BOTH, expand=True)

        # Top PanedWindow (DB1 and DB2 tables)
        self.top_pane = PanedWindow(self.main_pane, orient="horizontal")
        self.main_pane.add(self.top_pane, stretch="always")

        # --- DB1 Section ---
        self.db1_frame = Frame(self.top_pane)
        self.top_pane.add(self.db1_frame, minsize=200, stretch="always")

        self.db1_label = Label(self.db1_frame, text="Database 1 Path:")
        self.db1_label.pack(anchor="w")
        self.db1_entry = Entry(self.db1_frame, width=35)
        self.db1_entry.pack(fill=X)
        self.db1_entry.insert(0, r"C:\Nomis3\dbs\nomis3db")

        # Username, password, connect in one row
        self.db1_row = Frame(self.db1_frame)
        self.db1_row.pack(fill=X, pady=2)
        Label(self.db1_row, text="Username:").pack(side=LEFT)
        self.db1_user_entry = Entry(self.db1_row, width=10)
        self.db1_user_entry.pack(side=LEFT, padx=2)
        self.db1_user_entry.insert(0, "nomis")
        Label(self.db1_row, text="Password:").pack(side=LEFT)
        self.db1_pass_entry = Entry(self.db1_row, width=10, show="*")
        self.db1_pass_entry.pack(side=LEFT, padx=2)
        self.db1_pass_entry.insert(0, "nomispw")
        self.db1_connect_button = Button(self.db1_row, text="Connect", command=self.connect_db1)
        self.db1_connect_button.pack(side=LEFT, padx=2)

        self.db1_tables_label = Label(self.db1_frame, text="DB1 Tables")
        self.db1_tables_label.pack(anchor="w")
        self.db1_tables = ttk.Treeview(self.db1_frame, columns=("Table",), show="headings", height=15)
        self.db1_tables.heading("Table", text="Table Name")
        self.db1_tables.pack(fill=BOTH, expand=True)
        self.db1_tables.bind("<<TreeviewSelect>>", self.display_db1_table_content)
        self.db1_tables_scroll = Scrollbar(self.db1_frame, orient=VERTICAL, command=self.db1_tables.yview)
        self.db1_tables.configure(yscrollcommand=self.db1_tables_scroll.set)
        self.db1_tables_scroll.pack(side=RIGHT, fill=Y)

        # --- DB2 Section ---
        self.db2_frame = Frame(self.top_pane)
        self.top_pane.add(self.db2_frame, minsize=200, stretch="always")

        self.db2_label = Label(self.db2_frame, text="Database 2 Path:")
        self.db2_label.pack(anchor="w")
        self.db2_entry = Entry(self.db2_frame, width=35)
        self.db2_entry.pack(fill=X)
        self.db2_entry.insert(0, r"C:\Nomis3\dbs\nomis3db")

        # Username, password, connect in one row
        self.db2_row = Frame(self.db2_frame)
        self.db2_row.pack(fill=X, pady=2)
        Label(self.db2_row, text="Username:").pack(side=LEFT)
        self.db2_user_entry = Entry(self.db2_row, width=10)
        self.db2_user_entry.pack(side=LEFT, padx=2)
        self.db2_user_entry.insert(0, "nomis")
        Label(self.db2_row, text="Password:").pack(side=LEFT)
        self.db2_pass_entry = Entry(self.db2_row, width=10, show="*")
        self.db2_pass_entry.pack(side=LEFT, padx=2)
        self.db2_pass_entry.insert(0, "nomispw")
        self.db2_connect_button = Button(self.db2_row, text="Connect", command=self.connect_db2)
        self.db2_connect_button.pack(side=LEFT, padx=2)

        self.db2_tables_label = Label(self.db2_frame, text="DB2 Tables")
        self.db2_tables_label.pack(anchor="w")
        self.db2_tables = ttk.Treeview(self.db2_frame, columns=("Table",), show="headings", height=15)
        self.db2_tables.heading("Table", text="Table Name")
        self.db2_tables.pack(fill=BOTH, expand=True)
        self.db2_tables.bind("<<TreeviewSelect>>", self.display_db2_table_content)
        self.db2_tables_scroll = Scrollbar(self.db2_frame, orient=VERTICAL, command=self.db2_tables.yview)
        self.db2_tables.configure(yscrollcommand=self.db2_tables_scroll.set)
        self.db2_tables_scroll.pack(side=RIGHT, fill=Y)

        # --- Bottom PanedWindow (table content and controls) ---
        self.bottom_pane = PanedWindow(self.main_pane, orient="vertical")
        self.main_pane.add(self.bottom_pane, stretch="always")

        # --- Controls row above table content ---
        self.controls_frame = Frame(self.bottom_pane)
        self.bottom_pane.add(self.controls_frame, minsize=50, stretch="never")

        self.sql_text = Text(self.controls_frame, height=2, width=40)
        self.sql_text.pack(side=LEFT, padx=5, pady=2)
        self.sql_execute_button = Button(self.controls_frame, text="Execute SQL", command=self.execute_sql)
        self.sql_execute_button.pack(side=LEFT, padx=2)
        self.merge_button = Button(self.controls_frame, text="Merge Selected Table", command=self.merge_selected_table)
        self.merge_button.pack(side=LEFT, padx=2)
        self.prev_button = Button(self.controls_frame, text="Previous", command=self.prev_page)
        self.prev_button.pack(side=LEFT, padx=2)
        self.next_button = Button(self.controls_frame, text="Next", command=self.next_page)
        self.next_button.pack(side=LEFT, padx=2)
        self.page_label = Label(self.controls_frame, text="Page 1")
        self.page_label.pack(side=LEFT, padx=2)
        self.limit_label = Label(self.controls_frame, text="Limit:")
        self.limit_label.pack(side=LEFT)
        self.limit_entry = Entry(self.controls_frame, width=5)
        self.limit_entry.insert(0, "100")
        self.limit_entry.pack(side=LEFT, padx=2)

        # --- Table Content Section ---
        self.content_frame = Frame(self.bottom_pane)
        self.bottom_pane.add(self.content_frame, minsize=200, stretch="always")

        self.content_tree = ttk.Treeview(self.content_frame, show="headings")
        self.content_tree.pack(fill=BOTH, expand=True, side=LEFT)
        self.content_scroll_y = Scrollbar(self.content_frame, orient=VERTICAL, command=self.content_tree.yview)
        self.content_tree.config(yscrollcommand=self.content_scroll_y.set)
        self.content_scroll_y.pack(side=RIGHT, fill=Y)
        self.content_scroll_x = Scrollbar(self.content_frame, orient=HORIZONTAL, command=self.content_tree.xview)
        self.content_tree.config(xscrollcommand=self.content_scroll_x.set)
        self.content_scroll_x.pack(side=BOTTOM, fill=X)

        # Internal state
        self.db1_table_list = []
        self.db2_table_list = []
        self.selected_db1_table = None
        self.selected_db2_table = None

    def connect_db1(self):
        from main import DEFAULT_DRIVER_FOLDER, DEFAULT_DRIVER_CLASS
        path = self.db1_entry.get()
        user = self.db1_user_entry.get()
        password = self.db1_pass_entry.get()
        try:
            self.db1_connector = DerbyConnector(path, user, password, DEFAULT_DRIVER_FOLDER, DEFAULT_DRIVER_CLASS)
            self.db1_connector.connect()
            messagebox.showinfo("Success", "Connected to Database 1 successfully!")
            self.load_db1_tables()
        except Exception as e:
            messagebox.showerror("Connection Error", f"Failed to connect to DB1:\n{e}")

    def connect_db2(self):
        from main import DEFAULT_DRIVER_FOLDER, DEFAULT_DRIVER_CLASS
        path = self.db2_entry.get()
        user = self.db2_user_entry.get()
        password = self.db2_pass_entry.get()
        try:
            self.db2_connector = DerbyConnector(path, user, password, DEFAULT_DRIVER_FOLDER, DEFAULT_DRIVER_CLASS)
            self.db2_connector.connect()
            messagebox.showinfo("Success", "Connected to Database 2 successfully!")
            self.load_db2_tables()
        except Exception as e:
            messagebox.showerror("Connection Error", f"Failed to connect to DB2:\n{e}")

    def load_db1_tables(self):
        for row in self.db1_tables.get_children():
            self.db1_tables.delete(row)
        self.db1_table_list = []
        try:
            cursor = self.db1_connector.connection.cursor()
            cursor.execute("""
                SELECT s.schemaname, t.tablename
                FROM sys.systables t
                JOIN sys.sysschemas s ON t.schemaid = s.schemaid
                WHERE t.tabletype = 'T'
            """)
            self.db1_table_list = [(row[0], row[1]) for row in cursor.fetchall()]
            for schema, table in self.db1_table_list:
                self.db1_tables.insert("", "end", values=(f"{schema}.{table}",))
        except Exception as e:
            messagebox.showerror("Error", f"Failed to retrieve tables from DB1:\n{e}")

    def load_db2_tables(self):
        for row in self.db2_tables.get_children():
            self.db2_tables.delete(row)
        self.db2_table_list = []
        try:
            cursor = self.db2_connector.connection.cursor()
            cursor.execute("SELECT tablename FROM sys.systables WHERE tabletype='T'")
            self.db2_table_list = [row[0] for row in cursor.fetchall()]
            for table in self.db2_table_list:
                self.db2_tables.insert("", "end", values=(table,))
        except Exception as e:
            messagebox.showerror("Error", f"Failed to retrieve tables from DB2:\n{e}")

    def display_db1_table_content(self, event):
        selected = self.db1_tables.selection()
        if selected:
            table_name = self.db1_tables.item(selected[0])['values'][0]
            self.selected_db1_table = table_name
            self.display_table_content(self.db1_connector, table_name)

    def display_db2_table_content(self, event):
        selected = self.db2_tables.selection()
        if selected:
            table_name = self.db2_tables.item(selected[0])['values'][0]
            self.selected_db2_table = table_name
            self.display_table_content(self.db2_connector, table_name)

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

    def merge_selected_table(self):
        if not self.selected_db1_table or not self.selected_db2_table:
            messagebox.showerror("Error", "Please select a table from both DB1 and DB2 to merge.")
            return
        try:
            merge_logic = MergeLogic(self.db1_connector.connection, self.db2_connector.connection)
            merge_logic.merge_table(self.selected_db1_table, self.selected_db2_table)
            messagebox.showinfo("Success", f"Table '{self.selected_db1_table}' merged with '{self.selected_db2_table}' successfully!")
        except Exception as e:
            messagebox.showerror("Merge Error", str(e))

    def execute_sql(self):
        sql = self.sql_text.get("1.0", END).strip()
        if not sql:
            messagebox.showerror("Error", "Please enter an SQL query.")
            return
        # Decide which DB to run on based on last selected table
        connector = None
        if self.selected_db1_table:
            connector = self.db1_connector
        elif self.selected_db2_table:
            connector = self.db2_connector
        else:
            messagebox.showerror("Error", "Please select a table first.")
            return
        try:
            cursor = connector.connection.cursor()
            cursor.execute(sql)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                self.content_text.delete(1.0, END)
                self.content_text.insert(END, "\t".join(columns) + "\n")
                for row in rows:
                    self.content_text.insert(END, "\t".join(str(x) for x in row) + "\n")
            else:
                connector.connection.commit()
                self.content_text.delete(1.0, END)
                self.content_text.insert(END, "Query executed successfully.")
        except Exception as e:
            self.content_text.delete(1.0, END)
            self.content_text.insert(END, f"SQL Error: {e}")

    def next_page(self):
        if self.current_table and self.current_connector:
            self.display_table_content(self.current_connector, self.current_table, self.current_page + 1)

    def prev_page(self):
        if self.current_table and self.current_connector and self.current_page > 0:
            self.display_table_content(self.current_connector, self.current_table, self.current_page - 1)

    def update_table_content(self):
        if not self.current_connector or not self.current_connector.connection:
            return

        try:
            cursor = self.current_connector.connection.cursor()

            # Clear previous content
            for col in self.content_tree["columns"]:
                self.content_tree.heading(col, text="")
            self.content_tree.delete(*self.content_tree.get_children())

            # Fetch the data for the current page
            offset = self.current_page * self.page_size
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
            messagebox.showerror("Error", f"Error loading table: {e}")