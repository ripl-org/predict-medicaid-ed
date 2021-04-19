import matplotlib.pyplot as plt
import pandas as pd
import sys
from riipl import Connection

ed_claims, csv_file, pdf_file = sys.argv[1:]

sql = """
      SELECT pop.riipl_id,
             FLOOR(MONTHS_BETWEEN(claims.claim_dt, pop.first_dt)/12) AS "YEAR",
             SUM(claims.pay_amt)  AS pc_treatable,
             SUM(claims.bill_amt) AS pc_treatable_bill
        FROM (
              SELECT riipl_id,
                     MIN(claim_dt) AS first_dt
                FROM {ed_claims}
               WHERE pc_treatable = 1
            GROUP BY riipl_id
             ) pop
   LEFT JOIN {ed_claims} claims
          ON pop.riipl_id = claims.riipl_id
       WHERE claims.pc_treatable = 1
    GROUP BY pop.riipl_id, FLOOR(MONTHS_BETWEEN(claims.claim_dt, pop.first_dt)/12)
      """.format(**locals())

with Connection() as cxn:
    data = pd.read_sql(sql, cxn._connection)

data = data.pivot("RIIPL_ID", "YEAR", "PC_TREATABLE").fillna(0)

data = data.mean()
print(data)

data.to_csv(csv_file)

data.plot.line()
plt.tight_layout()
plt.savefig(pdf_file)

# vim: syntax=python expandtab sw=4 ts=4
