import pandas as pd
import os, sys, time
from riipl import CachePopulation, Connection, SaveFeatures

population, lookback, ri_unemp, out, manifest = sys.argv[1:]

def main():
    """
    Monthly unemployment rate in state of Rhode Island taken from Bureau of Labor and Statistics
    """

    index = ["SAMPLE_ID"]
    features = CachePopulation(population, index).set_index(index)

    sql = """
          SELECT pop.sample_id,
                 AVG(ru.unemp_rate) AS ri_unemployment
            FROM {population} pop
       LEFT JOIN {lookback} lb
              ON pop.quarter = lb.quarter
       LEFT JOIN {ri_unemp} ru
              ON lb.yrmo = ru.yrmo
        GROUP BY pop.sample_id
          """.format(**globals())

    with Connection() as cxn:
        features = features.join(pd.read_sql(sql, cxn._connection, index_col=index))

    labels = {"RI_UNEMPLOYMENT" : "Average Rhode Island unemployment rate"}

    SaveFeatures(features, out, manifest, population, labels)


# EXECUTE
if __name__ == "__main__":
    start = time.time()
    main()
    print("---%s seconds ---" % (time.time() - start))

# vim: expandtab sw=4 ts=4
