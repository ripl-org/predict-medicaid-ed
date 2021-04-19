import pandas as pd
import os, sys, time
from riipl import CachePopulation, Connection, SaveFeatures

population, lookback, mega, out, manifest = sys.argv[1:]

def main():
    """
    Social program payments from MEGA table
    """

    index = ["SAMPLE_ID"]
    features = CachePopulation(population, index).set_index(index)

    labels = {
        "SNAP_PAYMENTS"  : "Average monthly SNAP payments",
        "TANF_PAYMENTS"  : "Average monthly TANF payments",
        "TDI_PAYMENTS"   : "Average monthly TDI payments",
        "UI_PAYMENTS"    : "Average monthly UI payments"
    }

    with Connection() as cxn:
        for var in list(labels):
            sql = """
                  SELECT pop.sample_id,   
                         AVG(m.{var}) AS {var}
                    FROM {population} pop
               LEFT JOIN {lookback} lb
                      ON pop.quarter = lb.quarter
               LEFT JOIN {mega} m
                      ON pop.riipl_id = m.riipl_id AND
                         lb.yrmo = m.month
                   WHERE m.{var} > 0
                GROUP BY pop.sample_id
                """.format(var=var, **globals())
            features = features.join(pd.read_sql(sql, cxn._connection).set_index(index))

    # Indicators for non-zero values
    for var in list(labels):
        var_ind = var.partition("_")[0]
        features[var_ind] = features[var].notnull().astype(int)
        labels[var_ind] = "Received {}".format(var_ind)

    SaveFeatures(features, out, manifest, population, labels)


# EXECUTE
if __name__ == "__main__":
    start = time.time()
    main()
    print("---%s seconds ---" % (time.time() - start))
