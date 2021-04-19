import pandas as pd
import sys
from riipl import Connection

hcpcs_file, cpt_file, table = sys.argv[1:]

hcpcs = pd.read_csv(hcpcs_file, usecols=["PROC_CDE", "SECTION"], dtype=str)

cpt = pd.read_csv(cpt_file, dtype=str)

# Expand CPT ranges

proc_cde = []
section = []
for row in cpt[cpt.START.notnull()].itertuples():
    for i in range(int(row.START), int(row.END)+1):
        proc_cde.append("{:05d}".format(i))
        section.append(row.SECTION)

# Append CPT to HCPCS

hcpcs = hcpcs.append(pd.DataFrame({"PROC_CDE": proc_cde, "SECTION": section}), ignore_index=True)

with Connection() as cxn:
    cxn.read_dataframe(hcpcs, table)
    cxn.save_table(table, "PROC_CDE", checksum=False)

# vim: expandtab sw=4 ts=4
