import pandas as pd
import sys
from riipl import Connection

xlsx_file, table = sys.argv[1:]

with Connection() as cxn:
    cxn.read_dataframe(pd.read_excel(xlsx_file), table)
    cxn.save_table(table, checksum=False)

# vim: expandtab sw=4 ts=4
