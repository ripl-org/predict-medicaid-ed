import csv
import cx_Oracle
import hashlib
import inspect
import pandas as pd
import os
import re
import subprocess
import tempfile
from .sql_exceptions import *

class Connection:

    def __init__(self):
        self.schema = os.environ["USER"].upper()
        self._connection = cx_Oracle.connect("/")


    def _die_if_not_connected(self):
        if (self._connection is None):
            raise IllegalStateException("Action is not possible without a connection")


    def __enter__(self):
        return self


    def __exit__(self, exc_type, value, traceback):
        self.close()


    def close(self):
        self._die_if_not_connected()
        try:
            self._connection.close()
            self._connection = None
        except cx_Oracle.ProgrammingError:
            pass


    def _prepare_sql(self, sql):
        """
        Helper method for automating the process of pasting parameter values into 
        SQL queries
        """
        # IMPORTANT NOTE: If the nesting depth of _prepare_sql is changed the indexing on the
        # calling stack -must- be updated 
        caller = inspect.stack()[2][0]

        # Find and replace parameters
        def _find(key):
            for scope in [caller.f_locals, caller.f_globals, os.environ]:
                if key in scope:
                    return str(scope[key])
            raise NameError("Variable '{}' not found in local/global scope or ENV".format(key))

        for param in re.findall("(\%\w+\%)", sql):
            sql = sql.replace(param, _find(param.replace("%", "")))

        return sql


    def execute(self, sql, commit=True, verbose=False):
        """
        Execute a SQL statement and return the resulting cursor.
        """
        self._die_if_not_connected()
        cursor = self._connection.cursor()
        sql = self._prepare_sql(sql)
        if (verbose is True):
            print(sql)
        cursor.execute(sql)
        if (commit is True): 
            self._connection.commit()
        return cursor 


    def get_columns(self, table):
        """
        Return a (name, type) list of columns for the table.
        """
        sql = """
              SELECT column_name, data_type
                FROM user_tab_cols
               WHERE table_name = '{}'
            ORDER BY column_id
              """.format(table.upper())
        return self.execute(sql).fetchall()


    def get_checksum(self, table):
        """
        Calculate a checksum for a read-only table as a hash of all the
        fields concatenated with the column names and types.
        """
        cur = self.execute("""
                           SELECT column_name, data_type
                             FROM user_tab_columns
                            WHERE table_name = '{}'
                         ORDER BY column_id
                           """.format(table.upper()))
        columns = self.get_columns(table)
        if not columns: return ""
        m = hashlib.sha256()
        m.update(",".join(map("|".join, columns)).encode("utf-8"))
        table_hash = m.hexdigest()
        # Hash the columns values in each row, but sum the hashes across rows so
        # that row ordering does not impact the checksum.
        cur = self.execute("""
                           SELECT CAST(SUM(ORA_HASH({})) AS VARCHAR2(255))
                             FROM {}
                           """.format(" || '|' || ".join('"{}"'.format(c[0]) for c in columns),
                                      table.upper()))
        # Combine the data hash and the table schema hash
        checksum = "{}:{}".format(cur.fetchone()[0], table_hash)
        print("checksum for '{}': {}".format(table, checksum))
        return checksum


    def get_stats(self, table):
        """
        Returns a dataframe containing summary stats for each column in the table.
        """
        # Build and run queries for summary stats
        sql = []
        columns = self.get_columns(table)
        for col, dtype in columns:
            if (dtype == "NUMBER" or dtype == "LONG"):
                sql.append('COUNT("{0}"), AVG("{0}"), STDDEV("{0}"), MIN("{0}"), MAX("{0}")'.format(col))
            elif (dtype == "DATE"):
                sql.append('COUNT("{0}"), NULL, NULL, MIN("{0}"), MAX("{0}")'.format(col))
            else:
                sql.append('COUNT("{}"), NULL, NULL, NULL, NULL'.format(col))
        sql = "SELECT {} FROM {}".format(", ".join(sql), table)
        stats = self.execute(sql).fetchone()

        # Return them as a dataframe
        frame = pd.DataFrame(data=[x[0] for x in columns], columns=["Variable"])
        for i, stat in enumerate(["COUNT", "AVG", "STDDEV", "MIN", "MAX"]):
            frame[stat] = stats[i::5]
        return frame


    def clear_tables(self, tables, cascade_constraints=True):
        """
        Drops and purges tables.
        """
        # Convert bare input to list.
        if type(tables) is str:
            tables = [tables]
        if not isinstance(tables, list):
            raise TypeError("Argument 'tables' must be a string or list")

        options = "CASCADE CONSTRAINTS " if (cascade_constraints) else " "

        for table in tables:
            print("Clearing table:", table)
            try:
                self.execute("DROP TABLE %table% %options%PURGE")
            # cx_Oracle.DatabaseError: ORA-00942: table or view does not exist
            except cx_Oracle.DatabaseError as rc:
                if not "ORA-00942" in str(rc):
                    raise rc


    def create_pk(self, table, keys):
        """
        Create a primary key on table.
        """
        # Convert bare input to list.
        if type(keys) is str:
            keys = [keys]
        if not isinstance(keys, list):
            raise TypeError("Argument 'keys' must be a string or list")

        key_str = ", ".join(keys)
        key_name = "{}_PK".format(table.upper())
        self.execute("ALTER TABLE %table% ADD CONSTRAINT %key_name% PRIMARY KEY (%key_str%) DISABLE")
        self.execute("CREATE UNIQUE INDEX %key_name% ON %table% (%key_str%)")
        self.execute("ALTER TABLE %table% ENABLE PRIMARY KEY")


    def read_csv(self, filename, schema, table, delim=","):
        """
        Load a csv file with the provided schema into a SQL table with sqlldr.
        """
        # Determine file line endings
        with open(filename, "U") as f:
            f.readline()
            newline = repr(f.newlines)
        # Create a ctl file from the schema
        ctl_type = lambda x: x if (x.startswith("DATE") or x == "FILLER" or x == "RECNUM") else "CHAR"
        columns = ["{} {}".format(x[0], ctl_type(x[1])) for x in schema]
        ctl = """\
OPTIONS(
    ERRORS=0,
    SKIP=1,
    STREAMSIZE=1024000,
    DIRECT=TRUE,
    COLUMNARRAYROWS=20000,
    DATE_CACHE=20000,
    READSIZE=4194304,
    PARALLEL=TRUE
)
UNRECOVERABLE
LOAD DATA
INFILE '{}' "str {}"
APPEND
INTO TABLE {}
FIELDS TERMINATED BY '{}'
OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
{}
)
""".format(filename, newline, table, delim, ",\n".join(columns))
        fd, ctlfile = tempfile.mkstemp(prefix="riipl_connection_", suffix=".ctl")
        with open(fd, "w") as f:
            f.write(ctl)

        # Clear and create the sql table
        self.clear_tables(table)
        columns = ['"{}" {}'.format(x[0], x[1].partition(" ")[0].replace("RECNUM", "NUMBER")) for x in schema if x[1] is not "FILLER"]
        sql = "CREATE TABLE {} ({})".format(table, ",\n  ".join(columns))
        self.execute(sql, verbose=True)

        # Launch sqlldr with subprocess
        subprocess.check_call(["sqlldr",
                               "userid='/',control={0},log={0}.log".format(ctlfile, filename)])
        # Make table read-only
        self.execute("ALTER TABLE {} READ ONLY".format(table), verbose=True)

        # Store checksum of contents as the table's comment
        self.execute("COMMENT ON TABLE {} IS '{}'".format(table, self.get_checksum(table)))

        # Cleanup ctl file
        os.unlink(ctlfile)
        os.unlink(ctlfile + ".log")


    def read_dataframe(self, df, tablename, schema=None):
        """
        Load a pandas dataframe into a SQL table with sqlldr.
        """
        # Infer the schema from the pandas dtypes
        if schema is None:
            dtypes = {
                "object": "VARCHAR2(255)",
                "datetime64[ns]": "DATE 'YYYY-MM-DD'"
            }
            schema = [(c, dtypes.get(str(df.dtypes[c]), "NUMBER")) for c in df.columns]

        # Write csv to temporary file
        fd, csvfile = tempfile.mkstemp(prefix="riipl_connection_", suffix=".csv")
        with os.fdopen(fd, "w") as f:
            df.to_csv(f, index=False)

        # Load with sqlldr
        self.read_csv(csvfile, schema, tablename)

        # Cleanup csv file
        os.unlink(csvfile)


    def spool_to_csv(self, cursor, csv_path, header=True, BATCH_SIZE=10000):
        with open(csv_path, "w") as csv_f:
            writer = csv.writer(csv_f)

            if header:
                writer.writerow([column[0] for column in cursor.description])

            rows = cursor.fetchmany(BATCH_SIZE)
            while rows:
                writer.writerows(rows)
                rows = cursor.fetchmany(BATCH_SIZE)


    def save_table(self, table, key=None, checksum=True):
        """
        Helper method to finalize a permanent SQL table by creating a primary key,
        marking it read-only, and storing a checksum of the contents as a comment.

        Prints and returns summary statistics for the table.
        """
        # Create primary key
        if key is not None:
            self.create_pk(table, key)

        # Make table read-only
        try:
            self.execute("ALTER TABLE {} READ ONLY".format(table))
        except cx_Oracle.Error:
            # If the table is already read-only
            pass

        # Gather optimizer statistics
        self.execute("ANALYZE TABLE {} COMPUTE STATISTICS".format(table))

        # Store checksum of contents as the table's comment
        if checksum:
            self.execute("COMMENT ON TABLE {} IS '{}'".format(table, self.get_checksum(table)))

        # Print and return stats
        stats = self.get_stats(table)
        print("=" * 100)
        print("Table:", table)
        print("Key:", key)
        print("=" * 100)
        print(stats.to_string(index=False))
        print("\n")
        return stats


# vim: expandtab sw=4 ts=4
