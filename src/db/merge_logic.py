class MergeLogic:
    def __init__(self, db1_connection, db2_connection):
        self.db1_connection = db1_connection
        self.db2_connection = db2_connection

    def merge_table(self, table1, table2=None):
        """
        Merge records from table1 in db1_connection to table2 in db2_connection.
        If table2 is None, use table1 as the target table name.
        """
        if table2 is None:
            table2 = table1

        db1_cur = self.db1_connection.cursor()
        db2_cur = self.db2_connection.cursor()

        # Get columns
        db1_cur.execute(f'SELECT * FROM {table1} FETCH FIRST ROW ONLY')
        columns = [desc[0] for desc in db1_cur.description]
        col_list = ', '.join([f'"{col}"' for col in columns])

        # Fetch all data from source table
        db1_cur.execute(f'SELECT {col_list} FROM {table1}')
        rows = db1_cur.fetchall()

        # Insert into target table, skipping duplicates (simple version)
        placeholders = ', '.join(['?'] * len(columns))
        for row in rows:
            try:
                db2_cur.execute(f'INSERT INTO {table2} ({col_list}) VALUES ({placeholders})', row)
            except Exception:
                # You may want to handle duplicates or constraint errors here
                pass

        self.db2_connection.commit()