import pandas as pd
import sys
from riipl import Connection

outcomes, population, lookback, mega, subset = sys.argv[1:6]
feature_files = sys.argv[6:-1]
out_file = sys.argv[-1]

index = ["SAMPLE_ID"]

with Connection() as cxn:

    sql = """
          SELECT MEDIAN(future_spend)
            FROM {outcomes}
           WHERE subset = '{subset}' AND
                 future_spend > 0
          """.format(**locals())
    median = cxn.execute(sql).fetchone()[0]
    print("median:", median)

    sql = """
          SELECT out.sample_id,
                 CASE WHEN out.prior_visits >= 2        THEN 1 ELSE 0 END AS visits2,
                 CASE WHEN out.prior_visits >= 4        THEN 1 ELSE 0 END AS visits4,
                 CASE WHEN out.future_spend >  0        THEN 1 ELSE 0 END AS divertible,
                 CASE WHEN out.future_spend >  {median} THEN 1 ELSE 0 END AS upper_divertible,
                 out.prior_visits,
                 out.prior_spend,
                 out.future_spend,
                 a.age
            FROM {outcomes} out
       LEFT JOIN (
                  SELECT pop.sample_id,
                         MAX(m.age) AS age
                    FROM {population} pop
               LEFT JOIN {lookback} lb
                      ON pop.quarter = lb.quarter
               LEFT JOIN {mega} m
                      ON pop.riipl_id = m.riipl_id AND
                         lb.yrmo = m.month
                   WHERE pop.subset = '{subset}'
                GROUP BY pop.sample_id
                 ) a
              ON out.sample_id = a.sample_id
           WHERE out.subset = '{subset}'
          """.format(**locals())
    table = pd.read_sql(sql, cxn._connection, index_col=index)

for f in feature_files:
    if f.endswith("proc_cde.csv"):
        # Avoid loading entire procedures file
        cols = ["SAMPLE_ID", "MEDICAID_PROC_EM1", "MEDICAID_PROC_EM14", "MEDICAID_PROC_M7", "MEDICAID_PROC_M30"]
        table = table.join(pd.read_csv(f, usecols=cols, index_col=index))
    else:
        table = table.join(pd.read_csv(f, index_col=index))

# Adjustments
table["AGE1"] = (table["AGE"] < 18).astype(int)
table["AGE2"] = ((table["AGE"] >= 18) & (table["AGE"] <= 60)).astype(int)
table["AGE3"] = (table["AGE"] > 60).astype(int)
table["SEX_F"] = 1 - table["SEX_M"]
table["RACE_WHITE"] = 1 - table["RACE_BLACK"] - table["RACE_HISPANIC"] - table["RACE_OTHER"] - table["RACE_MISSING"]
table["MEDICAID_OUTPATIENT"] = table["MEDICAID_PROC_EM1"]
table["MEDICAID_ROUTINE"] = table["MEDICAID_PROC_EM14"] | table["MEDICAID_PROC_M7"] | table["MEDICAID_PROC_M30"]

columns = ["VISITS2", "VISITS4", "DIVERTIBLE", "UPPER_DIVERTIBLE"]

rows = ["AGE1", "AGE2", "AGE3", "SEX_F",
        "RACE_WHITE", "RACE_BLACK", "RACE_HISPANIC",
        "PRIOR_SPEND", "FUTURE_SPEND",
        "MEDICAID_MANAGED_CARE", "MEDICAID_PREM_PAYMENT",
        "MEDICAID_INST_PAY", "MEDICAID_PROF_PAY", "MEDICAID_RX_COST",
        "SNAP", "GEOCODED", "BLKGRP_MEDIANINCOME", "BLKGRP_BELOWFPL",
        "MEDICAID_DISEASE_SCORE", "MEDICAID_CHRONIC_SCORE", "MEDICAID_PROCEDURES",
        "MEDICAID_OUTPATIENT", "MEDICAID_ROUTINE"]

with open(out_file, "w") as f:
    print("\t".join(["Variable", "All"] + columns), file=f)
    # N
    print("\t".join(map(str, ["N", len(table)] + [table[x].sum() for x in columns])), file=f)
    # Means of remaining rows
    for row in rows:
        f.write("{}\t{:.3f}\t".format(row, table[row].mean()))
        print("\t".join(map("{:.3f}".format, [table.loc[table[x] == 1, row].mean() for x in columns])), file=f)

# vim: syntax=python expandtab sw=4 ts=4
