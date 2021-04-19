import pandas as pd
import os, sys, time
from riipl import CachePopulation, Connection, SaveFeatures

population, lookback, dim_date, medicaid_enrollment, claims, out, manifest = sys.argv[1:]

def main():

    index = ["SAMPLE_ID"]
    features = CachePopulation(population, index).set_index(index)

    sql = """
          SELECT enrolled.sample_id,
                 AVG(NVL(claims.inst_pay,    0)) AS medicaid_inst_pay,
                 AVG(NVL(claims.prof_pay,    0)) AS medicaid_prof_pay,
                 AVG(NVL(claims.nurs_pay,    0)) AS medicaid_nurs_pay,
                 AVG(NVL(claims.inst_bill,   0)) AS medicaid_inst_bill,
                 AVG(NVL(claims.prof_bill,   0)) AS medicaid_prof_bill,
                 AVG(NVL(claims.nurs_bill,   0)) AS medicaid_nurs_bill,
                 AVG(NVL(claims.inst_visits, 0)) AS medicaid_inst_visits,
                 AVG(NVL(claims.prof_visits, 0)) AS medicaid_prof_visits,
                 AVG(NVL(claims.nurs_visits, 0)) AS medicaid_nurs_visits
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
                         SUM(c.pay_amt  * c.inst) AS inst_pay,
                         SUM(c.pay_amt  * c.prof) AS prof_pay,
                         SUM(c.pay_amt  * c.nurs) AS nurs_pay,
                         SUM(c.bill_amt * c.inst) AS inst_bill,
                         SUM(c.bill_amt * c.prof) AS prof_bill,
                         SUM(c.bill_amt * c.nurs) AS nurs_bill,
                         SUM(c.inst)              AS inst_visits,
                         SUM(c.prof)              AS prof_visits,
                         SUM(c.nurs)              AS nurs_visits
                    FROM {population} pop
               LEFT JOIN {lookback} lb
                      ON pop.quarter = lb.quarter
               LEFT JOIN {dim_date} dd
                      ON lb.yrmo = dd.yrmo
              INNER JOIN {claims} c
                      ON pop.riipl_id = c.riipl_id AND
                         dd.date_dt = c.claim_dt
                GROUP BY pop.sample_id, lb.yrmo
                 ) claims
              ON enrolled.sample_id = claims.sample_id AND
                 enrolled.yrmo = claims.yrmo
        GROUP BY enrolled.sample_id
          """.format(**globals())

    with Connection() as cxn:
        features = features.join(pd.read_sql(sql, cxn._connection).set_index(index)).fillna(0)

    labels = {
        "MEDICAID_INST_PAY"    : "Average monthly medical payments to institutions",
        "MEDICAID_PROF_PAY"    : "Average monthly medical payments to professionals",
        "MEDICAID_NURS_PAY"    : "Average monthly medical payments to nursing homes",
        "MEDICAID_INST_BILL"   : "Average monthly medical bills from institutions",
        "MEDICAID_PROF_BILL"   : "Average monthly medical bills from professionals",
        "MEDICAID_NURS_BILL"   : "Average monthly medical bills from nursing homes",
        "MEDICAID_INST_VISITS" : "Average monthly medical visits to institutions",
        "MEDICAID_PROF_VISITS" : "Average monthly medical visits to professionals",
        "MEDICAID_NURS_VISITS" : "Average monthly medical visits to nursing homes"
    }

    SaveFeatures(features, out, manifest, population, labels)


# EXECUTE
if __name__ == "__main__":
    start = time.time()
    main()
    print("---%s seconds ---" % (time.time() - start))

# vim: expandtab sw=4 ts=4
