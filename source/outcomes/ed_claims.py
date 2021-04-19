import sys
from riipl import Connection

claims, rev_cde, mn_rev_cde, proc_cde, mn_proc_cde, table = sys.argv[1:]

sql = """
      CREATE TABLE %table% NOLOGGING PCTFREE 0 PARALLEL AS
      SELECT claims.*,
             NVL(pc.pc_treatable, 0) AS pc_treatable
        FROM (
              SELECT DISTINCT
                     riipl_id,
                     claim_dt
                FROM %claims% 
               WHERE riipl_id IS NOT NULL AND
                     (plc_svc_ed = 1 OR
                      (inst = 1 AND (rev_cde_ed = 1 OR proc_cde_ed = 1)))
             ) ed
   LEFT JOIN (
              SELECT riipl_id,
                     claim_dt,
                     COUNT(*)      AS n_claims,
                     SUM(pay_amt)  AS pay_amt,
                     SUM(bill_amt) AS bill_amt
                FROM %claims% 
               WHERE riipl_id IS NOT NULL AND
                     inst = 1 OR
                     (prof = 1 AND plc_svc_ed = 1)
            GROUP BY riipl_id, claim_dt
             ) claims
          ON ed.riipl_id = claims.riipl_id AND
             ed.claim_dt = claims.claim_dt
   LEFT JOIN (
              SELECT c.riipl_id,
                     c.claim_dt,
                     1 AS pc_treatable
                FROM %claims% c
           LEFT JOIN %rev_cde% r
                  ON c.claim_id = r.claim_id
           LEFT JOIN %mn_rev_cde% mr
                  ON r.rev_cde = mr.rev_cde
           LEFT JOIN %proc_cde% p
                  ON c.claim_id = p.claim_id
           LEFT JOIN %mn_proc_cde% mp
                  ON p.proc_cde = mp.proc_cde
               WHERE c.riipl_id IS NOT NULL AND
                     inst = 1 OR
                     (prof = 1 AND plc_svc_ed = 1)
            GROUP BY c.riipl_id, c.claim_dt
              HAVING MAX(mr.ed_indicator) <> 1 AND
                     MAX(mp.severe)       <> 1 AND
                     MAX(mp.ed_indicator) <> 1 AND
                     MAX(c.duration)      <= 1
             ) pc
          ON ed.riipl_id = pc.riipl_id AND
             ed.claim_dt = pc.claim_dt
      """

with Connection() as cxn:
    cxn.clear_tables(table)
    cxn.execute(sql)
    cxn.save_table(table, ["RIIPL_ID", "CLAIM_DT"])
      
# vim: expandtab sw=4 ts=4
