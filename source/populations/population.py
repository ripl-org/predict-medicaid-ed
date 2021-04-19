import pandas as pd
import sys
from riipl import Connection

medicaid_enrollment, lookback, table, out_file = sys.argv[1:]

sql = """
      CREATE TABLE %table% NOLOGGING PCTFREE 0 PARALLEL AS
      SELECT ROWNUM AS sample_id,
             quarter,
             riipl_id,
             subset
        FROM (
              SELECT DISTINCT
                     lb.quarter,
                     me.riipl_id,
                     lb.subset
                FROM %medicaid_enrollment% me
          INNER JOIN %lookback% lb
                  ON me.yrmo = lb.yrmo
               WHERE lb.month = 12
            ORDER BY lb.quarter, me.riipl_id
             )
      """

with Connection() as cxn:
    cxn.clear_tables(table)
    cxn.execute(sql)
    cxn.save_table(table, ["QUARTER", "RIIPL_ID"])
    sql = "SELECT * FROM {} ORDER BY sample_id".format(table)
    pd.read_sql(sql, cxn._connection).to_csv(out_file, index=False)

# vim: expandtab sw=4 ts=4
