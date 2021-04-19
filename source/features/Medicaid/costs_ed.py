import pandas as pd
import os, sys, time
from riipl import CachePopulation, Connection, SaveFeatures

population, lookback, dim_date, medicaid_enrollment, ed_claims, out, manifest = sys.argv[1:]

def main():

    index = ["SAMPLE_ID"]
    features = CachePopulation(population, index).set_index(index)

    # Claims payments

    sql = """
          SELECT enrolled.sample_id,
                 AVG(NVL(claims.pay,       0)) AS medicaid_ed_pay,
                 AVG(NVL(claims.pc_pay,    0)) AS medicaid_ed_pc_pay,
                 AVG(NVL(claims.bill,      0)) AS medicaid_ed_bill,
                 AVG(NVL(claims.pc_bill,   0)) AS medicaid_ed_pc_bill,
                 AVG(NVL(claims.visits,    0)) AS medicaid_ed_visits,
                 AVG(NVL(claims.pc_visits, 0)) AS medicaid_ed_pc_visits
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
                         SUM(ec.pay_amt)                    AS pay,
                         SUM(ec.pay_amt  * ec.pc_treatable) AS pc_pay,
                         SUM(ec.bill_amt)                   AS bill,
                         SUM(ec.bill_amt * ec.pc_treatable) AS pc_bill,
                         COUNT(*)                           AS visits,
                         SUM(ec.pc_treatable)               AS pc_visits
                    FROM {population} pop
               LEFT JOIN {lookback} lb
                      ON pop.quarter = lb.quarter
               LEFT JOIN {dim_date} dd
                      ON lb.yrmo = dd.yrmo
              INNER JOIN {ed_claims} ec
                      ON pop.riipl_id = ec.riipl_id AND
                         dd.date_dt = ec.claim_dt
                GROUP BY pop.sample_id, lb.yrmo
                 ) claims
              ON enrolled.sample_id = claims.sample_id AND
                 enrolled.yrmo = claims.yrmo
        GROUP BY enrolled.sample_id
          """.format(**globals())

    with Connection() as cxn:
        features = features.join(pd.read_sql(sql, cxn._connection).set_index(index)).fillna(0)

    labels = {
        "MEDICAID_ED_PAY"       : "Average monthly ED payments",
        "MEDICAID_ED_PC_PAY"    : "Average monthly PC-treatable ED payments",
        "MEDICAID_ED_BILL"      : "Average monthly ED bills",
        "MEDICAID_ED_PC_BILL"   : "Average monthly PC-treatable ED bills",
        "MEDICAID_ED_VISITS"    : "Average monthly ED visits",
        "MEDICAID_ED_PC_VISITS" : "Average monthly PC-tretable ED visits"
    }

    SaveFeatures(features, out, manifest, population, labels)


# EXECUTE
if __name__ == "__main__":
    start = time.time()
    main()
    print("---%s seconds ---" % (time.time() - start))

# vim: expandtab sw=4 ts=4
