class MergeLogic:
    def __init__(self, db1_connection, db2_connection):
        self.db1_connection = db1_connection
        self.db2_connection = db2_connection # This is the source

    def merge_table(self, table1, table2=None, log_callback=None):
        """
        Append all records from table2 in db2_connection to table1 in db1_connection.
        For auto-increment columns, assign the next available value in db1.
        Schema is always included (default APP).
        """
        # table1 is target (db1), table2 is source (db2)
        if table2 is None:
            table2 = table1

        # Extract schema and table names for db1
        if '.' in table1:
            schema1, tbl1 = table1.split('.', 1)
        else:
            schema1, tbl1 = 'APP', table1
        db1_table = f'"{schema1}"."{tbl1}"'

        # Extract schema and table names for db2
        if '.' in table2:
            schema2, tbl2 = table2.split('.', 1)
        else:
            schema2, tbl2 = 'APP', table2
        db2_table = f'"{schema2}"."{tbl2}"'

        db1_cur = self.db1_connection.cursor()
        db2_cur = self.db2_connection.cursor()

        # Get all columns from db1 table
        db1_cur.execute(f'SELECT * FROM {db1_table} FETCH FIRST ROW ONLY')
        db1_columns = [desc[0] for desc in db1_cur.description]

        if log_callback:
            log_callback(f"db1_columns: {db1_columns}")

        # Find auto-increment columns in db1
        tbl1_upper = tbl1.upper()
        db1_cur.execute(f"""
            SELECT c.columnname
            FROM sys.syscolumns c
            JOIN sys.systables t ON c.referenceid = t.tableid
            WHERE t.tablename = '{tbl1_upper}'
              AND c.autoincrementvalue IS NOT NULL
        """)
        auto_inc_cols = [row[0] for row in db1_cur.fetchall()]

        # Prepare insert statement for non-auto-increment columns
        insert_columns = [col for col in db1_columns if col not in auto_inc_cols]
        col_list = ', '.join([f'"{col}"' for col in insert_columns])
        placeholders = ', '.join(['?'] * len(insert_columns))

        # Fetch all data from db2
        db2_cur.execute(f'SELECT * FROM {db2_table}')
        db2_columns = [desc[0] for desc in db2_cur.description]
        db2_rows = db2_cur.fetchall()

        # Map db2 columns to the columns we will be inserting into db1
        try:
            col_indexes = [db2_columns.index(col) for col in insert_columns]
        except ValueError as e:
            raise Exception(f"Column mismatch between source and target tables: {e}")

        inserted = 0
        for row in db2_rows:
            # Map source row to target column order, excluding auto-increment columns
            values = list(row[i] for i in col_indexes)
            try:
                db1_cur.execute(f'INSERT INTO {db1_table} ({col_list}) VALUES ({placeholders})', values)
                inserted += 1
                # Reduce logging verbosity
                # if log_callback and inserted % 100 == 0:
                #     log_callback(f"{inserted} rows merged so far...")
            except Exception as e:
                if log_callback:
                    log_callback(f"Insert error: {e} | Values: {values}")

        self.db1_connection.commit()
        if log_callback:
            log_callback(f"Merge complete. {inserted} rows inserted.")
        return inserted