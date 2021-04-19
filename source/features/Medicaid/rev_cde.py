import pandas as pd
import os, sys, time
from riipl import CachePopulation, Connection, SaveFeatures

population, lookback, dim_date, claims, rev_cde, rev_file, out, manifest = sys.argv[1:]

def main():

    rev = pd.read_csv(rev_file, dtype=str)

    index = ["SAMPLE_ID"]
    features = CachePopulation(population, index).set_index(index)

    sql = """
          SELECT DISTINCT
                 pop.sample_id,
                 SUBSTR(r.rev_cde, 1, 3) AS rev_cde
            FROM {population} pop
       LEFT JOIN {lookback} lb
              ON pop.quarter = lb.quarter
       LEFT JOIN {dim_date} dd
              ON lb.yrmo = dd.yrmo
      INNER JOIN {claims} c
              ON pop.riipl_id = c.riipl_id AND
                 dd.date_dt = c.claim_dt
      INNER JOIN {rev_cde} r
              ON c.claim_id = r.claim_id
          """.format(**globals())

    with Connection() as cxn:
        revs = pd.read_sql(sql, cxn._connection)

    features = features.join(pd.crosstab(revs.SAMPLE_ID, revs.REV_CDE)).fillna(0)
    features.columns = list(map("MEDICAID_REV_{}".format, features.columns))
    
    labels = dict(("MEDICAID_REV_{}".format(row.REV_CDE.upper()),
                   "Revenue code for '{}'".format(row.DESCRIPTION))
                  for row in rev.itertuples())

    features = features[[c for c in features.columns if c in labels]]

    SaveFeatures(features, out, manifest, population, labels)


# EXECUTE
if __name__ == "__main__":
    start = time.time()
    main()
    print("---%s seconds ---" % (time.time() - start))

# vim: expandtab sw=4 ts=4
