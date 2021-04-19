"""
Calculate prior and future divertible spending, to identify "cost-emergents"
who move from zero prior spending to top 10% future spending.
"""

import pandas as pd
import sys
from riipl import Connection

population, lookback, lookahead, dim_date, ed_claims, table, out_file = sys.argv[1:]

index = "SAMPLE_ID"

with Connection() as cxn:

    sql = """
          SELECT sample_id,
                 subset,
                 quarter
            FROM {}
          """.format(population)
    outcomes = pd.read_sql(sql, cxn._connection, index_col=index)

    sql = """
          SELECT pop.sample_id,
                 COUNT(ec.pay_amt)               AS prior_visits,
                 SUM(ec.pay_amt*ec.pc_treatable) AS prior_spend
            FROM {population} pop
       LEFT JOIN {lookback} lb
              ON pop.quarter = lb.quarter
       LEFT JOIN {dim_date} dd
              ON lb.yrmo = dd.yrmo
      INNER JOIN {ed_claims} ec
              ON pop.riipl_id = ec.riipl_id AND
                 dd.date_dt = ec.claim_dt
        GROUP BY pop.sample_id
          """.format(**locals())
    outcomes = outcomes.join(pd.read_sql(sql, cxn._connection, index_col=index)).fillna(0)

    sql = """
          SELECT pop.sample_id,
                 SUM(ec.pay_amt) AS future_spend,
                 TO_NUMBER(TO_CHAR(MIN(ec.claim_dt), 'YYYYMMDD')) AS first_ed_dt
            FROM {population} pop
       LEFT JOIN {lookahead} la
              ON pop.quarter = la.quarter
       LEFT JOIN {dim_date} dd
              ON la.yrmo = dd.yrmo
      INNER JOIN {ed_claims} ec
              ON pop.riipl_id = ec.riipl_id AND
                 dd.date_dt = ec.claim_dt
           WHERE ec.pc_treatable = 1
        GROUP BY pop.sample_id
          """.format(**locals())
    outcomes = outcomes.join(pd.read_sql(sql, cxn._connection, index_col=index)).fillna(0)

    outcomes["COST_EMERGENCE"] = ((outcomes.PRIOR_SPEND == 0) & (outcomes.FUTURE_SPEND > 0)).astype(int)
    outcomes.sort_index(inplace=True)

    cxn.read_dataframe(outcomes.reset_index(), table)
    cxn.save_table(table, index, checksum=False)

outcomes.to_csv(out_file)

# vim: expandtab sw=4 ts=4
