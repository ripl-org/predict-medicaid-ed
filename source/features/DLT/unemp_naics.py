import cx_Oracle
import pandas as pd
import numpy as np
import os, sys, time
from riipl import SaveFeatures

population, dlt_wage, unemp_file, out, manifest = sys.argv[1:]

def main():
    """
    Nationwide unemployment rate by NAICS code at a yearly level from Bureau of Labor and Statistics
    """
    index = ["SAMPLE_ID"]

    unemp = pd.read_csv(unemp_file)
    unemp["NAICS"] = unemp.NAICS.astype(str)

    # Calculate average unemployment by year
    unemp_avg = unemp.groupby("YR").mean().reset_index()
    print(unemp_avg)

    # Lookup all wage records in the lookback period, and order by
    # wages, to select the NAICS for the highest wage.
    sql = """
          SELECT DISTINCT
                 pop.sample_id,
                 pop.quarter,
                 dw.wages,
                 SUBSTR(dw.naics4, 1, 2) AS naics
            FROM {population} pop
       LEFT JOIN {dlt_wage} dw
              ON pop.riipl_id = dw.riipl_id AND
                 pop.quarter = TO_NUMBER(dw.yyq)
        ORDER BY pop.sample_id, dw.wages
           """.format(**globals())
 
    with cx_Oracle.connect("/") as cxn:
        features = pd.read_sql(sql, cxn).drop_duplicates(index, keep="last").set_index(index)

    # Use the previous year for joining the unemployment rate
    features["YR"] = (2000 + features.QUARTER / 10).astype(int)

    # Join NAICS unemployment, using avg unemployment when no NAICS is available
    naics = features.merge(unemp, how="left", on=["YR", "NAICS"]).NAICS_UNEMP_RATE

    avg = features.merge(unemp_avg, how="left", on="YR").NAICS_UNEMP_RATE

    features["NAICS_UNEMPLOYMENT"] = np.where(naics.notnull(), naics, avg)

    labels = {"NAICS_UNEMPLOYMENT": "National annual unemployment rate for worker's industry"}

    SaveFeatures(features[["NAICS_UNEMPLOYMENT"]], out, manifest, population, labels)


# EXECUTE
if __name__ == "__main__":
    start = time.time()
    main()
    print("---%s seconds ---" % (time.time() - start))

# vim: expandtab sw=4 ts=4
