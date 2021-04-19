import pandas as pd
import sys
from riipl import Connection

ed_claims, start, end, out_file = sys.argv[1:]

sql = """
      SELECT EXTRACT(YEAR FROM claim_dt)         AS year,
             ROUND(SUM(pay_amt))                 AS ed_total,
             ROUND(SUM(pay_amt * pc_treatable))  AS pc_treatable,
             ROUND(SUM(bill_amt * pc_treatable)) AS pc_treatable_bill
        FROM {ed_claims}
       WHERE EXTRACT(YEAR FROM claim_dt) BETWEEN {start} AND {end}
    GROUP BY EXTRACT(YEAR FROM claim_dt)
    ORDER BY EXTRACT(YEAR FROM claim_dt)
      """.format(**locals())

with Connection() as cxn:
    pd.read_sql(sql, cxn._connection).to_csv(out_file, index=False)

# vim: syntax=python expandtab sw=4 ts=4
