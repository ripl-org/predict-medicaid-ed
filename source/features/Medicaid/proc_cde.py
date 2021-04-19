import pandas as pd
import os, sys, time
from riipl import CachePopulation, Connection, SaveFeatures

population, lookback, dim_date, proc_cde, hcpcs, section_file, out, manifest = sys.argv[1:]

def main():

    sections = pd.read_csv(section_file, usecols=["SECTION", "DESCRIPTION"]).drop_duplicates()

    index = ["SAMPLE_ID"]
    features = CachePopulation(population, index).set_index(index)

    sql = """
          SELECT DISTINCT
                 pop.sample_id,
                 h.section AS section
            FROM {population} pop
       LEFT JOIN {lookback} lb
              ON pop.quarter = lb.quarter
       LEFT JOIN {dim_date} dd
              ON lb.yrmo = dd.yrmo
      INNER JOIN {proc_cde} p
              ON pop.riipl_id = p.riipl_id AND
                 dd.date_dt = p.claim_dt
      INNER JOIN {hcpcs} h
              ON p.proc_cde = h.proc_cde
           WHERE h.section IS NOT NULL
          """.format(**globals())

    with Connection() as cxn:
        procs = pd.read_sql(sql, cxn._connection)

    features = features.join(pd.crosstab(procs.SAMPLE_ID, procs.SECTION)).fillna(0)
    features.columns = list(map("MEDICAID_PROC_{}".format, features.columns))

    labels = dict(("MEDICAID_PROC_{}".format(row.SECTION),
                   "Procedure for '{}'".format(row.DESCRIPTION))
                  for row in sections.itertuples())

    SaveFeatures(features, out, manifest, population, labels, bool_features=list(labels))


# EXECUTE
if __name__ == "__main__":
    start = time.time()
    main()
    print("---%s seconds ---" % (time.time() - start))

# vim: expandtab sw=4 ts=4
