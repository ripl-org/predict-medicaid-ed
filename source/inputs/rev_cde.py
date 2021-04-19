import sys
from riipl import Connection

enc_837_inst, ffs_inst, ffs_instx, ed_mc_inst, table = sys.argv[1:]

sql = """
      CREATE TABLE %table% PCTFREE 0 NOLOGGING PARALLEL AS

      --- Source #1: ENC_837_INST

      SELECT re_unique_id*100000000000 + cl_id_sak*100 + 1 AS claim_id,
                                                              rev_cde 
        FROM %enc_837_inst%
       WHERE rev_cde IS NOT NULL

       UNION

      --- Source #2: FFS_INST

      SELECT re_unique_id*100000000000 + cl_id_sak*100 + 2 AS claim_id,
                                                              rev_cde 
        FROM %ffs_inst%
       WHERE rev_cde IS NOT NULL AND
             cl_id_sak > 0

       UNION

      --- Source #3: FFS_INSTX

      SELECT re_unique_id*100000000000 + cl_id_sak*100 + 3 AS claim_id,
                                                              rev_cde 
        FROM %ffs_instx%
       WHERE rev_cde IS NOT NULL AND
             cl_id_sak > 0

       UNION

      --- Source #9: ED_MC_INST

      SELECT recipient_unique_id*100000000000 + record_id*100 + 9 AS claim_id,
             revenue_code_1                                       AS rev_cde
        FROM %ed_mc_inst%
       WHERE revenue_code_1 IS NOT NULL AND
             recipient_unique_id > 0

       UNION

      SELECT recipient_unique_id*100000000000 + record_id*100 + 9 AS claim_id,
             revenue_code_2                                       AS rev_cde
        FROM %ed_mc_inst%
       WHERE revenue_code_2 IS NOT NULL AND
             recipient_unique_id > 0

       UNION

      SELECT recipient_unique_id*100000000000 + record_id*100 + 9 AS claim_id,
             revenue_code_3                                       AS rev_cde
        FROM %ed_mc_inst%
       WHERE revenue_code_3 IS NOT NULL AND
             recipient_unique_id > 0

       UNION

      SELECT recipient_unique_id*100000000000 + record_id*100 + 9 AS claim_id,
             revenue_code_4                                       AS rev_cde
        FROM %ed_mc_inst%
       WHERE revenue_code_4 IS NOT NULL AND
             recipient_unique_id > 0

       UNION

      SELECT recipient_unique_id*100000000000 + record_id*100 + 9 AS claim_id,
             revenue_code_5                                       AS rev_cde
        FROM %ed_mc_inst%
       WHERE revenue_code_5 IS NOT NULL AND
             recipient_unique_id > 0
      """

with Connection() as cxn:
    cxn.clear_tables(table)
    cxn.execute(sql)
    cxn.save_table(table)

# vim: expandtab sw=4 ts=4
