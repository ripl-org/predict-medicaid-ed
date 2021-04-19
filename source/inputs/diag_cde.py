import sys
from riipl import Connection

enc_837_inst, ffs_inst, ffs_instx, \
enc_837_prof, ffs_prof, ffs_profx, \
enc_837_nurs_hm, ffs_nurs_hm, \
ed_mc_inst, ed_mc_prof, ed_rhp_prof, \
recip_x_ssn, table = sys.argv[1:]

sql = """
      CREATE TABLE %table% PCTFREE 0 NOLOGGING PARALLEL AS
      SELECT DISTINCT
             rxn.riipl_id,
             claims.claim_dt,
             TRIM(claims.diag_cde) AS diag_cde
        FROM (
              --- Source #1: ENC_837_INST

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, cl_diag_cde AS diag_cde
                FROM %enc_837_inst%
               WHERE cl_diag_cde IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_1 AS diag_cde
                FROM %enc_837_inst%
               WHERE diag_cde_1 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_2 AS diag_cde
                FROM %enc_837_inst%
               WHERE diag_cde_2 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_3 AS diag_cde
                FROM %enc_837_inst%
               WHERE diag_cde_3 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_4 AS diag_cde
                FROM %enc_837_inst%
               WHERE diag_cde_4 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_5 AS diag_cde
                FROM %enc_837_inst%
               WHERE diag_cde_5 IS NOT NULL

               UNION

              --- Source #2: FFS_INST

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, cl_diag_cde AS diag_cde
                FROM %ffs_inst%
               WHERE cl_diag_cde IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_1 AS diag_cde
                FROM %ffs_inst%
               WHERE diag_cde_1 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_2 AS diag_cde
                FROM %ffs_inst%
               WHERE diag_cde_2 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_3 AS diag_cde
                FROM %ffs_inst%
               WHERE diag_cde_3 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_4 AS diag_cde
                FROM %ffs_inst%
               WHERE diag_cde_4 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_5 AS diag_cde
                FROM %ffs_inst%
               WHERE diag_cde_5 IS NOT NULL

               UNION

              --- Source #3: FFS_INSTX

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, cl_diag_cde AS diag_cde
                FROM %ffs_instx%
               WHERE cl_diag_cde IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_1 AS diag_cde
                FROM %ffs_instx%
               WHERE diag_cde_1 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_2 AS diag_cde
                FROM %ffs_instx%
               WHERE diag_cde_2 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_3 AS diag_cde
                FROM %ffs_instx%
               WHERE diag_cde_3 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_4 AS diag_cde
                FROM %ffs_instx%
               WHERE diag_cde_4 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_5 AS diag_cde
                FROM %ffs_instx%
               WHERE diag_cde_5 IS NOT NULL

               UNION

              --- Source #4: ENC_837_PROF

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, cl_diag_cde AS diag_cde
                FROM %enc_837_prof%
               WHERE cl_diag_cde IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_1 AS diag_cde
                FROM %enc_837_prof%
               WHERE diag_cde_1 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_2 AS diag_cde
                FROM %enc_837_prof%
               WHERE diag_cde_2 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_3 AS diag_cde
                FROM %enc_837_prof%
               WHERE diag_cde_3 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_4 AS diag_cde
                FROM %enc_837_prof%
               WHERE diag_cde_4 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_5 AS diag_cde
                FROM %enc_837_prof%
               WHERE diag_cde_5 IS NOT NULL

               UNION

              --- Source #5: FFS_PROF

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, cl_diag_cde AS diag_cde
                FROM %ffs_prof%
               WHERE cl_diag_cde IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_1 AS diag_cde
                FROM %ffs_prof%
               WHERE diag_cde_1 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_2 AS diag_cde
                FROM %ffs_prof%
               WHERE diag_cde_2 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_3 AS diag_cde
                FROM %ffs_prof%
               WHERE diag_cde_3 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_4 AS diag_cde
                FROM %ffs_prof%
               WHERE diag_cde_4 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_5 AS diag_cde
                FROM %ffs_prof%
               WHERE diag_cde_5 IS NOT NULL

               UNION

              --- Source #6: FFS_PROFX

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, cl_diag_cde AS diag_cde
                FROM %ffs_profx%
               WHERE cl_diag_cde IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_1 AS diag_cde
                FROM %ffs_profx%
               WHERE diag_cde_1 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_2 AS diag_cde
                FROM %ffs_profx%
               WHERE diag_cde_2 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_3 AS diag_cde
                FROM %ffs_profx%
               WHERE diag_cde_3 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_4 AS diag_cde
                FROM %ffs_profx%
               WHERE diag_cde_4 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_5 AS diag_cde
                FROM %ffs_profx%
               WHERE diag_cde_5 IS NOT NULL

               UNION

              --- Source #7: ENC_837_NURS_HM

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, cl_diag_cde AS diag_cde
                FROM %enc_837_nurs_hm%
               WHERE cl_diag_cde IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_1 AS diag_cde
                FROM %enc_837_nurs_hm%
               WHERE diag_cde_1 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_2 AS diag_cde
                FROM %enc_837_nurs_hm%
               WHERE diag_cde_2 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_3 AS diag_cde
                FROM %enc_837_nurs_hm%
               WHERE diag_cde_3 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_4 AS diag_cde
                FROM %enc_837_nurs_hm%
               WHERE diag_cde_4 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_5 AS diag_cde
                FROM %enc_837_nurs_hm%
               WHERE diag_cde_5 IS NOT NULL

               UNION

              --- Source #8: FFS_NURS_HM

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, cl_diag_cde AS diag_cde
                FROM %ffs_nurs_hm%
               WHERE cl_diag_cde IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_1 AS diag_cde
                FROM %ffs_nurs_hm%
               WHERE diag_cde_1 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_2 AS diag_cde
                FROM %ffs_nurs_hm%
               WHERE diag_cde_2 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_3 AS diag_cde
                FROM %ffs_nurs_hm%
               WHERE diag_cde_3 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_4 AS diag_cde
                FROM %ffs_nurs_hm%
               WHERE diag_cde_4 IS NOT NULL

               UNION

              SELECT re_unique_id, cl_from_dte_svc AS claim_dt, diag_cde_5 AS diag_cde
                FROM %ffs_nurs_hm%
               WHERE diag_cde_5 IS NOT NULL

               UNION

              --- Source #9: ED_MC_INST

              SELECT recipient_unique_id AS re_unique_id, admission_date AS claim_dt, diag_code_1 AS diag_cde
                FROM %ed_mc_inst%
               WHERE diag_code_1 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, admission_date AS claim_dt, diag_code_2 AS diag_cde
                FROM %ed_mc_inst%
               WHERE diag_code_2 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, admission_date AS claim_dt, diag_code_3 AS diag_cde
                FROM %ed_mc_inst%
               WHERE diag_code_3 IS NOT NULL

               UNION
 
              SELECT recipient_unique_id AS re_unique_id, admission_date AS claim_dt, diag_code_4 AS diag_cde
                FROM %ed_mc_inst%
               WHERE diag_code_4 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, admission_date AS claim_dt, diag_code_5 AS diag_cde
                FROM %ed_mc_inst%
               WHERE diag_code_5 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, admission_date AS claim_dt, diag_code_6 AS diag_cde
                FROM %ed_mc_inst%
               WHERE diag_code_6 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, admission_date AS claim_dt, diag_code_7 AS diag_cde
                FROM %ed_mc_inst%
               WHERE diag_code_7 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, admission_date AS claim_dt, diag_code_8 AS diag_cde
                FROM %ed_mc_inst%
               WHERE diag_code_8 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, admission_date AS claim_dt, diag_code_9 AS diag_cde
                FROM %ed_mc_inst%
               WHERE diag_code_9 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, admission_date AS claim_dt, diag_code_10 AS diag_cde
                FROM %ed_mc_inst%
               WHERE diag_code_10 IS NOT NULL

               UNION

              --- Source #10: ED_MC_PROF

              SELECT recipient_unique_id AS re_unique_id, service_date AS claim_dt, diag_code_1 AS diag_cde
                FROM %ed_mc_prof%
               WHERE diag_code_1 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, service_date AS claim_dt, diag_code_2 AS diag_cde
                FROM %ed_mc_prof%
               WHERE diag_code_2 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, service_date AS claim_dt, diag_code_3 AS diag_cde
                FROM %ed_mc_prof%
               WHERE diag_code_3 IS NOT NULL

               UNION
 
              SELECT recipient_unique_id AS re_unique_id, service_date AS claim_dt, diag_code_4 AS diag_cde
                FROM %ed_mc_prof%
               WHERE diag_code_4 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, service_date AS claim_dt, diag_code_5 AS diag_cde
                FROM %ed_mc_prof%
               WHERE diag_code_5 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, service_date AS claim_dt, diag_code_6 AS diag_cde
                FROM %ed_mc_prof%
               WHERE diag_code_6 IS NOT NULL

               UNION

              --- Source #11: ED_RHP_PROF

              SELECT recipient_unique_id AS re_unique_id, service_date AS claim_dt, diag_code_1 AS diag_cde
                FROM %ed_rhp_prof%
               WHERE diag_code_1 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, service_date AS claim_dt, diag_code_2 AS diag_cde
                FROM %ed_rhp_prof%
               WHERE diag_code_2 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, service_date AS claim_dt, diag_code_3 AS diag_cde
                FROM %ed_rhp_prof%
               WHERE diag_code_3 IS NOT NULL

               UNION
 
              SELECT recipient_unique_id AS re_unique_id, service_date AS claim_dt, diag_code_4 AS diag_cde
                FROM %ed_rhp_prof%
               WHERE diag_code_4 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, service_date AS claim_dt, diag_code_5 AS diag_cde
                FROM %ed_rhp_prof%
               WHERE diag_code_5 IS NOT NULL

               UNION

              SELECT recipient_unique_id AS re_unique_id, service_date AS claim_dt, diag_code_6 AS diag_cde
                FROM %ed_rhp_prof%
               WHERE diag_code_6 IS NOT NULL
 
             ) claims
   LEFT JOIN %recip_x_ssn% rxn
          ON claims.re_unique_id = rxn.recipient_id
      """

with Connection() as cxn:
    cxn.clear_tables(table)
    cxn.execute(sql)
    cxn.save_table(table)

# vim: expandtab sw=4 ts=4
