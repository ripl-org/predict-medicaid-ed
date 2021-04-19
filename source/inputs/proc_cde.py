import sys
from riipl import Connection

enc_837_inst, ffs_inst, ffs_instx, \
enc_837_prof, ffs_prof, ffs_profx, \
ed_mc_inst, ed_mc_prof, ed_rhp_prof, \
recip_x_ssn, table = sys.argv[1:]

sql = """
      CREATE TABLE %table% PCTFREE 0 NOLOGGING PARALLEL AS
      SELECT DISTINCT
             rxn.riipl_id,
             claims.claim_id,
             claims.claim_dt,
             claims.proc_cde,
             claims.source
        FROM (
              --- Source #1: ENC_837_INST

              SELECT                                                  re_unique_id,
                     re_unique_id*100000000000 + cl_id_sak*100 + 1 AS claim_id,
                     cl_from_dte_svc                               AS claim_dt,
                                                                      proc_cde,
                     'INST'                                        AS source
                FROM %enc_837_inst%
               WHERE proc_cde IS NOT NULL

               UNION

              --- Source #2: FFS_INST

              SELECT                                                  re_unique_id,
                     re_unique_id*100000000000 + cl_id_sak*100 + 2 AS claim_id,
                     cl_from_dte_svc                               AS claim_dt,
                                                                      proc_cde,
                     'INST'                                        AS source
                FROM %ffs_inst%
               WHERE proc_cde IS NOT NULL

               UNION

              --- Source #3: FFS_INSTX

              SELECT                                                  re_unique_id,
                     re_unique_id*100000000000 + cl_id_sak*100 + 3 AS claim_id,
                     cl_from_dte_svc                               AS claim_dt,
                                                                      proc_cde,
                     'INST'                                        AS source
                FROM %ffs_instx%
               WHERE proc_cde IS NOT NULL

               UNION

              --- Source #4: ENC_837_PROF

              SELECT                                                  re_unique_id,
                     re_unique_id*100000000000 + cl_id_sak*100 + 4 AS claim_id,
                     cl_from_dte_svc                               AS claim_dt,
                                                                      proc_cde,
                     'PROF'                                        AS source
                FROM %enc_837_prof%
               WHERE proc_cde IS NOT NULL

               UNION

              --- Source #5: FFS_PROF

              SELECT                                                  re_unique_id,
                     re_unique_id*100000000000 + cl_id_sak*100 + 5 AS claim_id,
                     cl_from_dte_svc                               AS claim_dt,
                                                                      proc_cde,
                     'PROF'                                        AS source
                FROM %ffs_prof%
               WHERE proc_cde IS NOT NULL

               UNION

              --- Source #6: FFS_PROFX

              SELECT                                                  re_unique_id,
                     re_unique_id*100000000000 + cl_id_sak*100 + 6 AS claim_id,
                     cl_from_dte_svc                               AS claim_dt,
                                                                      proc_cde,
                     'PROF'                                        AS source
                FROM %ffs_profx%
               WHERE proc_cde IS NOT NULL

               UNION

              --- Source #9: ED_MC_INST

              SELECT recipient_unique_id                                  AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 9 AS claim_id,
                     admission_date                                       AS claim_dt,
                     proc_code_1                                          AS proc_cde,
                     'INST'                                               AS source
                FROM %ed_mc_inst%
               WHERE proc_code_1 IS NOT NULL

               UNION

              SELECT recipient_unique_id                                  AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 9 AS claim_id,
                     admission_date                                       AS claim_dt,
                     proc_code_2                                          AS proc_cde,
                     'INST'                                               AS source
                FROM %ed_mc_inst%
               WHERE proc_code_2 IS NOT NULL

               UNION

              SELECT recipient_unique_id                                  AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 9 AS claim_id,
                     admission_date                                       AS claim_dt,
                     proc_code_3                                          AS proc_cde,
                     'INST'                                               AS source
                FROM %ed_mc_inst%
               WHERE proc_code_3 IS NOT NULL

               UNION

              SELECT recipient_unique_id                                  AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 9 AS claim_id,
                     admission_date                                       AS claim_dt,
                     proc_code_4                                          AS proc_cde,
                     'INST'                                               AS source
                FROM %ed_mc_inst%
               WHERE proc_code_4 IS NOT NULL

               UNION

              SELECT recipient_unique_id                                  AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 9 AS claim_id,
                     admission_date                                       AS claim_dt,
                     proc_code_5                                          AS proc_cde,
                     'INST'                                               AS source
                FROM %ed_mc_inst%
               WHERE proc_code_5 IS NOT NULL

               UNION

              SELECT recipient_unique_id                                  AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 9 AS claim_id,
                     admission_date                                       AS claim_dt,
                     proc_code_6                                          AS proc_cde,
                     'INST'                                               AS source
                FROM %ed_mc_inst%
               WHERE proc_code_6 IS NOT NULL

               UNION
 
              --- Source #10: ED_MC_PROF

              SELECT recipient_unique_id                                   AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 10 AS claim_id,
                     service_date                                          AS claim_dt,
                     proc_code_1                                           AS proc_cde,
                     'PROF'                                                AS source
                FROM %ed_mc_prof%
               WHERE proc_code_1 IS NOT NULL

               UNION

              SELECT recipient_unique_id                                   AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 10 AS claim_id,
                     service_date                                          AS claim_dt,
                     proc_code_2                                           AS proc_cde,
                     'PROF'                                                AS source
                FROM %ed_mc_prof%
               WHERE proc_code_2 IS NOT NULL

               UNION
 
              SELECT recipient_unique_id                                   AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 10 AS claim_id,
                     service_date                                          AS claim_dt,
                     proc_code_3                                           AS proc_cde,
                     'PROF'                                                AS source
                FROM %ed_mc_prof%
               WHERE proc_code_3 IS NOT NULL

               UNION

              SELECT recipient_unique_id                                   AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 10 AS claim_id,
                     service_date                                          AS claim_dt,
                     proc_code_4                                           AS proc_cde,
                     'PROF'                                                AS source
                FROM %ed_mc_prof%
               WHERE proc_code_4 IS NOT NULL

               UNION

              SELECT recipient_unique_id                                   AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 10 AS claim_id,
                     service_date                                          AS claim_dt,
                     proc_code_5                                           AS proc_cde,
                     'PROF'                                                AS source
                FROM %ed_mc_prof%
               WHERE proc_code_5 IS NOT NULL

               UNION

              --- Source #11: ED_RHP_PROF

              SELECT recipient_unique_id                                   AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 11 AS claim_id,
                     service_date                                          AS claim_dt,
                     proc_code_1                                           AS proc_cde,
                     'PROF'                                                AS source
                FROM %ed_rhp_prof%
               WHERE proc_code_1 IS NOT NULL

               UNION

              SELECT recipient_unique_id                                   AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 11 AS claim_id,
                     service_date                                          AS claim_dt,
                     proc_code_2                                           AS proc_cde,
                     'PROF'                                                AS source
                FROM %ed_rhp_prof%
               WHERE proc_code_2 IS NOT NULL

               UNION
 
              SELECT recipient_unique_id                                   AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 11 AS claim_id,
                     service_date                                          AS claim_dt,
                     proc_code_3                                           AS proc_cde,
                     'PROF'                                                AS source
                FROM %ed_rhp_prof%
               WHERE proc_code_3 IS NOT NULL

               UNION

              SELECT recipient_unique_id                                   AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 11 AS claim_id,
                     service_date                                          AS claim_dt,
                     proc_code_4                                           AS proc_cde,
                     'PROF'                                                AS source
                FROM %ed_rhp_prof%
               WHERE proc_code_4 IS NOT NULL

               UNION

              SELECT recipient_unique_id                                   AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 11 AS claim_id,
                     service_date                                          AS claim_dt,
                     proc_code_5                                           AS proc_cde,
                     'PROF'                                                AS source
                FROM %ed_rhp_prof%
               WHERE proc_code_5 IS NOT NULL

             ) claims
   LEFT JOIN %recip_x_ssn% rxn
          ON claims.re_unique_id = rxn.recipient_id
      """

with Connection() as cxn:
    cxn.clear_tables(table)
    cxn.execute(sql)
    cxn.save_table(table)

# vim: expandtab sw=4 ts=4
