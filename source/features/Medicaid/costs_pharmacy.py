import pandas as pd
import os, sys, time
from riipl import CachePopulation, Connection, SaveFeatures

population, lookback, dim_date, medicaid_enrollment, pharmacy, out, manifest = sys.argv[1:]

def main():

    index = ["SAMPLE_ID"]
    features = CachePopulation(population, index).set_index(index)

    sql = """
          SELECT enrolled.sample_id,
                 AVG(NVL(pharmacy.cost,   0)) AS medicaid_rx_cost,
                 AVG(NVL(pharmacy.drugs,  0)) AS medicaid_rx_drugs
            FROM (
                  SELECT DISTINCT
                         pop.sample_id,
                         lb.yrmo
                    FROM {population} pop
               LEFT JOIN {lookback} lb
                      ON pop.quarter = lb.quarter
              INNER JOIN {medicaid_enrollment} me
                      ON pop.riipl_id = me.riipl_id AND
                         lb.yrmo = me.yrmo
                 ) enrolled
       LEFT JOIN (
                  SELECT pop.sample_id,
                         lb.yrmo,
                         SUM(p.pay_amt)              AS cost,
                         COUNT(DISTINCT p.ndc9_code) AS drugs
                    FROM {population} pop
               LEFT JOIN {lookback} lb
                      ON pop.quarter = lb.quarter
               LEFT JOIN {dim_date} dd
                      ON lb.yrmo = dd.yrmo
              INNER JOIN {pharmacy} p
                      ON pop.riipl_id = p.riipl_id AND
                         dd.date_dt = p.dispensed_dt
                GROUP BY pop.sample_id, lb.yrmo
                 ) pharmacy
              ON enrolled.sample_id = pharmacy.sample_id AND
                 enrolled.yrmo = pharmacy.yrmo
        GROUP BY enrolled.sample_id
          """.format(**globals())

    with Connection() as cxn:
        features = features.join(pd.read_sql(sql, cxn._connection).set_index(index)).fillna(0)

    labels = {
        "MEDICAID_RX_COST"     : "Average monthly pharmcy costs",
        "MEDICAID_RX_DRUGS"    : "Average monthly number of prescription drugs"
    }

    SaveFeatures(features, out, manifest, population, labels)


# EXECUTE
if __name__ == "__main__":
    start = time.time()
    main()
    print("---%s seconds ---" % (time.time() - start))

# vim: expandtab sw=4 ts=4
