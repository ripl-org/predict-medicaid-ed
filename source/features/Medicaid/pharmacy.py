import numpy as np
import pandas as pd
import os, sys, time
from collections import defaultdict
from riipl import *

population, lookback, dim_date, pharmacy, ashp, out, manifest = sys.argv[1:]

def main():

    index = ["SAMPLE_ID"]
    features = CachePopulationSubsets(population, index).set_index(index)

    # Load ASHP descriptions
    sql = """
          SELECT DISTINCT "CLASS", "DESC"
            FROM {ashp}
           WHERE "CLASS" IS NOT NULL
          """.format(**globals())
    with Connection() as cxn:
        ashp_desc = pd.read_sql(sql, cxn._connection, index_col="CLASS")

    # Load pharmacy claims
    sql = """ 
          SELECT sample_id,
                 "CLASS"
            FROM (
                  SELECT DISTINCT
                         pop.sample_id,
                         'ASHP_' || NVL(a."CLASS", 'MISSING') AS "CLASS"
                    FROM {population} pop
               LEFT JOIN {lookback} lb
                      ON pop.quarter = lb.quarter
               LEFT JOIN {dim_date} dd
                      ON lb.yrmo = dd.yrmo
              INNER JOIN {pharmacy} p
                      ON pop.riipl_id = p.riipl_id AND
                         dd.date_dt = p.dispensed_dt
               LEFT JOIN {ashp} a
                      ON p.ndc9_code = a.ndc9_code
                 )
        GROUP BY sample_id, "CLASS"
          """.format(**globals())

    with Connection() as cxn:
        values = pd.read_sql(sql, cxn._connection)

    # Pivot ASHP categories
    values["VALUE"] = 1
    values = values.pivot("SAMPLE_ID", "CLASS", "VALUE")

    features = features.join(values).fillna(0)

    labels = {}
    for c in features.columns[1:]:
        if c == "ASHP_MISSING":
            labels[c] = "Prescription for unknown drug category"
        elif c.startswith("ASHP_99"):
            labels[c] = "Prescription for unknown drug category"
        else:
            labels[c] = "Prescription for '{}'".format(ashp_desc.loc[c[5:], "DESC"])

    # Merge perfectly correlated categories
    training = features[features.SUBSET == "TRAIN"]
    varnames = set(features.columns[1:])
    groups = defaultdict(set)
    while varnames:
        var1 = varnames.pop()
        for var2 in list(varnames):
            if training[var1].equals(training[var2]):
                groups[var1].add(var2)
                varnames.remove(var2)
    for k, v in groups.items():
        group = [k] + list(v)
        print("merging perfectly correlated categories:", ", ".join(group))
        merged = "ASHP_" + "_".join(var[5:] for var in group)
        features[merged] = features[group[0]]
        for var in group:
            del features[var]
        labels[merged] = labels[group[0]] + " and related drugs"

    del features["SUBSET"]

    SaveFeatures(features, out, manifest, population, labels, bool_features=list(labels))


# EXECUTE
if __name__ == "__main__":
    start = time.time()
    main()
    print("---%s seconds ---" % (time.time() - start))

# vim: expandtab sw=4 ts=4
