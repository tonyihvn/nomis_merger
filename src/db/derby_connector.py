import os


class DerbyConnector:
    def __init__(self, db_path, user, password, driver_folder, driver_class="org.apache.derby.iapi.jdbc.AutoloadedDriver"):
        self.db_path = db_path
        self.user = user
        self.password = password
        self.driver_folder = driver_folder
        self.driver_class = driver_class
        self.connection = None

    def connect(self):
        import jaydebeapi

        # Collect all .jar files in the driver folder
        jar_dir = os.path.abspath(self.driver_folder)
        jar_files = [os.path.join(jar_dir, f) for f in os.listdir(jar_dir) if f.endswith('.jar')]

        url = f"jdbc:derby:{self.db_path};create=true"
        self.connection = jaydebeapi.connect(
            self.driver_class,
            url,
            [self.user, self.password],
            jar_files
        )
        print("Connected to the database.")

    def execute_query(self, query, params=None):
        if self.connection is None:
            print("No database connection established.")
            return None
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        except Exception as e:
            print(f"Error executing query: {e}")
        finally:
            cursor.close()

    def close(self):
        if self.connection:
            self.connection.close()
            print("Database connection closed.")