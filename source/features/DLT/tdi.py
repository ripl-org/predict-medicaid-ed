import pandas as pd
import os, sys, time
from riipl import CachePopulation, Connection, SaveFeatures

population, lookback, dim_date, dlt_tdi, icd9_file, out, manifest = sys.argv[1:]

def main():

    icd9 = pd.read_csv(icd9_file)

    index = ["SAMPLE_ID"]
    features = CachePopulation(population, index).set_index(index)

    sql = """
          SELECT DISTINCT
                 pop.sample_id,
                 pop.subset,
                 morbidity_code AS diag_cde
            FROM {population} pop
       LEFT JOIN {lookback} lb
              ON pop.quarter = lb.quarter
       LEFT JOIN {dim_date} dd
              ON lb.yrmo = dd.yrmo
      INNER JOIN {dlt_tdi} dt
              ON pop.riipl_id = dt.riipl_id AND
                 dd.date_dt = dt.unable_dte
           WHERE dt.morbidity_code IS NOT NULL
          """.format(**globals())

    with Connection() as cxn:
        diags = pd.read_sql(sql, cxn._connection)

    print(len(diags), "diagnoses")

    # Retain codes that exist in at least 0.1% of the training population
    codes = diags[diags.SUBSET == "TRAIN"].DIAG_CDE.value_counts(normalize=True)
    del diags["SUBSET"]
    codes = codes[codes >= 0.001]
    print(codes)

    diags = diags[diags.DIAG_CDE.isin(codes.index)]
    print("retained", len(diags), "diagnoses at >=0.001 frequency")

    features = features.join(pd.crosstab(diags.SAMPLE_ID, diags.DIAG_CDE)).fillna(0)
    features.columns = list(map("TDI_DIAG_{}".format, features.columns))
    
    labels = dict(("TDI_DIAG_{}".format(row.diag_cde.upper()),
                   "TDI claim for '{}'".format(row.description))
                  for row in icd9.itertuples())

    features = features[[c for c in features.columns if c in labels]]

    SaveFeatures(features, out, manifest, population, labels)


# EXECUTE
if __name__ == "__main__":
    start = time.time()
    main()
    print("---%s seconds ---" % (time.time() - start))

# vim: expandtab sw=4 ts=4
