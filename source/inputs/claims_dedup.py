import sys
from riipl import Connection

claims, table = sys.argv[1:]

sql = """
      CREATE TABLE %table% PCTFREE 0 NOLOGGING PARALLEL AS
      SELECT riipl_id,
             re_unique_id,
             MAX(claim_id) AS claim_id,
             claim_dt,
             MAX(claim_type) AS claim_type,
             claim_bill_type,
             duration,
             (pay_amt + medcr_amt) AS pay_amt,
             GREATEST(bill_amt, pay_amt + medcr_amt) AS bill_amt,
             inst,
             prof,
             nurs,
             mc,
             ffs,
             crossover,
             MAX(provider_id) AS provider_id,
             MAX(plc_svc_ed) AS plc_svc_ed,
             MAX(proc_cde_ed) AS proc_cde_ed,
             MAX(rev_cde_ed) AS rev_cde_ed
        FROM %claims%
       WHERE riipl_id IS NOT NULL AND
             pay_amt >= 0 AND medcr_amt >= 0
    GROUP BY riipl_id, re_unique_id, claim_dt, claim_bill_type, duration,
             pay_amt, medcr_amt, bill_amt, inst, prof, nurs, mc, ffs, crossover
      """

with Connection() as cxn:
    cxn.clear_tables(table)
    cxn.execute(sql)
    cxn.save_table(table, "CLAIM_ID")

# vim: expandtab sw=4 ts=4
