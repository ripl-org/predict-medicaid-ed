import cx_Oracle
import pandas as pd
import os, sys, time
from riipl import CachePopulation, SaveFeatures

population, dlt_wage, out, manifest = sys.argv[1:]

def main():
    """
    Summary of income and employers from DLT wages
    """

    index = ["SAMPLE_ID"]
    features = CachePopulation(population, index).set_index(index)

    sql = """
          SELECT pop.sample_id,
                 SUM(dw.wages)          AS wages,
                 SUM(dw.hrs_worked)     AS wage_hours,
                 COUNT(DISTINCT dw.ern) AS employers
            FROM {population} pop
      INNER JOIN {dlt_wage} dw
              ON pop.riipl_id = dw.riipl_id AND
                 pop.quarter = TO_NUMBER(dw.yyq)
        GROUP BY pop.sample_id
          """.format(**globals())

    with cx_Oracle.connect("/") as cxn:
        features = features.join(pd.read_sql(sql, cxn).set_index(index))

    # Indicator for non-zero wages
    features["HAS_WAGES"] = features.WAGES.notnull().astype(int)

    labels = {
        "WAGES"      : "Wages in previous quarter",
        "EMPLOYERS"  : "Number of employers in previous quarter",
        "WAGE_HOURS" : "Hours worked in previous quarter",
        "HAS_WAGES"  : "Has Rhode Island wages in previous quarter"
    }

    SaveFeatures(features, out, manifest, population, labels)


# EXECUTE
if __name__ == "__main__":
    start = time.time()
    main()
    print("---%s seconds ---" % (time.time() - start))

# vim: expandtab sw=4 ts=4
