import sys
from riipl import Connection

enc_837_inst, ffs_inst, ffs_instx, \
enc_837_prof, ffs_prof, ffs_profx, \
enc_837_nurs_hm, ffs_nurs_hm, \
ed_mc_inst, ed_mc_prof, ed_rhp_prof, \
recip_x_ssn, rev_cde, proc_cde, table = sys.argv[1:]

sql = """
      CREATE TABLE %table% PCTFREE 0 NOLOGGING PARALLEL AS
      SELECT rxn.riipl_id,
             claims.*,
             NVL(rev_cde.ed, 0) AS rev_cde_ed,
             NVL(proc_cde.ed, 0) AS proc_cde_ed
        FROM (
              --- Source #1: ENC_837_INST

              SELECT                                                  re_unique_id,
                     re_unique_id*100000000000 + cl_id_sak*100 + 1 AS claim_id,
                     MIN(cl_from_dte_svc)                          AS claim_dt,
                     STATS_MODE(cl_typ_cde)                        AS claim_type,
                     STATS_MODE(cl_typ_bill)                       AS claim_bill_type,
                     MAX(cl_to_dte_svc) - MIN(cl_from_dte_svc)     AS duration,
                     SUM(cl_pd_amt)                                AS pay_amt,
                     MAX(cl_medcr_pd_amt)                          AS medcr_amt,
                     SUM(cl_dtl_bill_amt)                          AS bill_amt,
                     1                                             AS inst,
                     0                                             AS prof,
                     0                                             AS nurs,
                     1                                             AS mc,
                     0                                             AS ffs,
                     0                                             AS crossover,
                     TO_CHAR(STATS_MODE(bill_npi_id))              AS provider_id,
                     MAX(CASE WHEN plc_svc_cde = '23' THEN 1
                              WHEN plc_svc_cde = '41' THEN 1
                              ELSE 0 END)                          AS plc_svc_ed
                FROM %enc_837_inst%
               WHERE cl_id_sak > 0
            GROUP BY re_unique_id, cl_id_sak

               UNION ALL

              --- Source #2: FFS_INST

              SELECT                                                  re_unique_id,
                     re_unique_id*100000000000 + cl_id_sak*100 + 2 AS claim_id,
                     MIN(cl_from_dte_svc)                          AS claim_dt,
                     STATS_MODE(cl_typ_cde)                        AS claim_type,
                     STATS_MODE(cl_typ_bill)                       AS claim_bill_type,
                     MAX(cl_to_dte_svc) - MIN(cl_from_dte_svc)     AS duration,
                     SUM(cl_pd_amt)                                AS pay_amt,
                     MAX(cl_medcr_pd_amt)                          AS medcr_amt,
                     SUM(cl_dtl_bill_amt)                          AS bill_amt,
                     1                                             AS inst,
                     0                                             AS prof,
                     0                                             AS nurs,
                     0                                             AS mc,
                     1                                             AS ffs,
                     0                                             AS crossover,
                     TO_CHAR(STATS_MODE(bill_npi_id))              AS provider_id,
                     MAX(CASE WHEN plc_svc_cde = '23' THEN 1
                              WHEN plc_svc_cde = '41' THEN 1
                              ELSE 0 END)                          AS plc_svc_ed
                FROM %ffs_inst%
               WHERE cl_id_sak > 0
            GROUP BY re_unique_id, cl_id_sak

               UNION ALL

              --- Source #3: FFS_INSTX

              SELECT                                                  re_unique_id,
                     re_unique_id*100000000000 + cl_id_sak*100 + 3 AS claim_id,
                     MIN(cl_from_dte_svc)                          AS claim_dt,
                     STATS_MODE(cl_typ_cde)                        AS claim_type,
                     STATS_MODE(cl_typ_bill)                       AS claim_bill_type,
                     MAX(cl_to_dte_svc) - MIN(cl_from_dte_svc)     AS duration,
                     SUM(cl_pd_amt)                                AS pay_amt,
                     MAX(cl_medcr_pd_amt)                          AS medcr_amt,
                     SUM(cl_dtl_bill_amt)                          AS bill_amt,
                     1                                             AS inst,
                     0                                             AS prof,
                     0                                             AS nurs,
                     0                                             AS mc,
                     1                                             AS ffs,
                     1                                             AS crossover,
                     TO_CHAR(STATS_MODE(bill_npi_id))              AS provider_id,
                     MAX(CASE WHEN plc_svc_cde = '23' THEN 1
                              WHEN plc_svc_cde = '41' THEN 1
                              ELSE 0 END)                          AS plc_svc_ed
                FROM %ffs_instx%
               WHERE cl_id_sak > 0
            GROUP BY re_unique_id, cl_id_sak

               UNION ALL

              --- Source #4: ENC_837_PROF

              SELECT                                                  re_unique_id,
                     re_unique_id*100000000000 + cl_id_sak*100 + 4 AS claim_id,
                     MIN(cl_from_dte_svc)                          AS claim_dt,
                     STATS_MODE(cl_typ_cde)                        AS claim_type,
                     STATS_MODE(cl_typ_bill)                       AS claim_bill_type,
                     MAX(cl_to_dte_svc) - MIN(cl_from_dte_svc)     AS duration,
                     SUM(cl_pd_amt)                                AS pay_amt,
                     MAX(cl_medcr_pd_amt)                          AS medcr_amt,
                     SUM(cl_dtl_bill_amt)                          AS bill_amt,
                     0                                             AS inst,
                     1                                             AS prof,
                     0                                             AS nurs,
                     1                                             AS mc,
                     0                                             AS ffs,
                     0                                             AS crossover,
                     TO_CHAR(STATS_MODE(bill_npi_id))              AS provider_id,
                     MAX(CASE WHEN plc_svc_cde = '23' THEN 1
                              WHEN plc_svc_cde = '41' THEN 1
                              ELSE 0 END)                          AS plc_svc_ed
                FROM %enc_837_prof%
               WHERE cl_id_sak > 0
            GROUP BY re_unique_id, cl_id_sak

               UNION ALL

              --- Source #5: FFS_PROF

              SELECT                                                  re_unique_id,
                     re_unique_id*100000000000 + cl_id_sak*100 + 5 AS claim_id,
                     MIN(cl_from_dte_svc)                          AS claim_dt,
                     STATS_MODE(cl_typ_cde)                        AS claim_type,
                     STATS_MODE(cl_typ_bill)                       AS claim_bill_type,
                     MAX(cl_to_dte_svc) - MIN(cl_from_dte_svc)     AS duration,
                     SUM(cl_pd_amt)                                AS pay_amt,
                     MAX(cl_medcr_pd_amt)                          AS medcr_amt,
                     SUM(cl_dtl_bill_amt)                          AS bill_amt,
                     0                                             AS inst,
                     1                                             AS prof,
                     0                                             AS nurs,
                     0                                             AS mc,
                     1                                             AS ffs,
                     0                                             AS crossover,
                     TO_CHAR(STATS_MODE(bill_npi_id))              AS provider_id,
                     MAX(CASE WHEN plc_svc_cde = '23' THEN 1
                              WHEN plc_svc_cde = '41' THEN 1
                              ELSE 0 END)                          AS plc_svc_ed
                FROM %ffs_prof%
               WHERE cl_id_sak > 0
            GROUP BY re_unique_id, cl_id_sak

               UNION ALL

              --- Source #6: FFS_PROFX

              SELECT                                                  re_unique_id,
                     re_unique_id*100000000000 + cl_id_sak*100 + 6 AS claim_id,
                     MIN(cl_from_dte_svc)                          AS claim_dt,
                     STATS_MODE(cl_typ_cde)                        AS claim_type,
                     STATS_MODE(cl_typ_bill)                       AS claim_bill_type,
                     MAX(cl_to_dte_svc) - MIN(cl_from_dte_svc)     AS duration,
                     SUM(cl_pd_amt)                                AS pay_amt,
                     MAX(cl_medcr_pd_amt)                          AS medcr_amt,
                     SUM(cl_dtl_bill_amt)                          AS bill_amt,
                     0                                             AS inst,
                     1                                             AS prof,
                     0                                             AS nurs,
                     0                                             AS mc,
                     1                                             AS ffs,
                     1                                             AS crossover,
                     TO_CHAR(STATS_MODE(bill_npi_id))              AS provider_id,
                     MAX(CASE WHEN plc_svc_cde = '23' THEN 1
                              WHEN plc_svc_cde = '41' THEN 1
                              ELSE 0 END)                          AS plc_svc_ed
                FROM %ffs_profx%
               WHERE cl_id_sak > 0
            GROUP BY re_unique_id, cl_id_sak

               UNION ALL

              --- Source #7: ENC_837_NURS_HM

              SELECT                                                  re_unique_id,
                     re_unique_id*100000000000 + cl_id_sak*100 + 7 AS claim_id,
                     MIN(cl_from_dte_svc)                          AS claim_dt,
                     STATS_MODE(cl_typ_cde)                        AS claim_type,
                     STATS_MODE(cl_typ_bill)                       AS claim_bill_type,
                     MAX(cl_to_dte_svc) - MIN(cl_from_dte_svc)     AS duration,
                     SUM(cl_pd_amt)                                AS pay_amt,
                     MAX(cl_medcr_pd_amt)                          AS medcr_amt,
                     SUM(cl_dtl_bill_amt)                          AS bill_amt,
                     0                                             AS inst,
                     0                                             AS prof,
                     1                                             AS nurs,
                     1                                             AS mc,
                     0                                             AS ffs,
                     0                                             AS crossover,
                     TO_CHAR(STATS_MODE(bill_npi_id))              AS provider_id,
                     MAX(CASE WHEN plc_svc_cde = '23' THEN 1
                              WHEN plc_svc_cde = '41' THEN 1
                              ELSE 0 END)                          AS plc_svc_ed

                FROM %enc_837_nurs_hm%
               WHERE cl_id_sak > 0
            GROUP BY re_unique_id, cl_id_sak

               UNION ALL

              --- Source #8: FFS_NURS_HM

              SELECT                                                  re_unique_id,
                     re_unique_id*100000000000 + cl_id_sak*100 + 8 AS claim_id,
                     MIN(cl_from_dte_svc)                          AS claim_dt,
                     STATS_MODE(cl_typ_cde)                        AS claim_type,
                     STATS_MODE(cl_typ_bill)                       AS claim_bill_type,
                     MAX(cl_to_dte_svc) - MIN(cl_from_dte_svc)     AS duration,
                     SUM(cl_pd_amt)                                AS pay_amt,
                     MAX(cl_medcr_pd_amt)                          AS medcr_amt,
                     SUM(cl_dtl_bill_amt)                          AS bill_amt,
                     0                                             AS inst,
                     0                                             AS prof,
                     1                                             AS nurs,
                     0                                             AS mc,
                     1                                             AS ffs,
                     0                                             AS crossover,
                     TO_CHAR(STATS_MODE(bill_npi_id))              AS provider_id,
                     MAX(CASE WHEN plc_svc_cde = '23' THEN 1
                              WHEN plc_svc_cde = '41' THEN 1
                              ELSE 0 END)                          AS plc_svc_ed
                FROM %ffs_nurs_hm%
               WHERE cl_id_sak > 0
            GROUP BY re_unique_id, cl_id_sak

               UNION ALL

              --- Source #9: ED_MC_INST

              SELECT recipient_unique_id                                  AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 9 AS claim_id,
                     admission_date                                       AS claim_dt,
                     NULL                                                 AS claim_type,
                     bill_type                                            AS claim_bill_type,
                     discharge_date - admission_date                      AS duration,
                     net_payment                                          AS pay_amt,
                     0                                                    AS medcr_amt,
                     total_charges                                        AS bill_amt,
                     1                                                    AS inst,
                     0                                                    AS prof,
                     0                                                    AS nurs,
                     1                                                    AS mc,
                     0                                                    AS ffs,
                     0                                                    AS crossover,
                     license_state ||
                     license_number ||
                     provider_license_type                                AS provider_id,
                     NULL                                                 AS plc_svc_ed
                FROM %ed_mc_inst%
               WHERE recipient_unique_id > 0

               UNION ALL

              --- Source #10: ED_MC_PROF

              SELECT recipient_unique_id                                   AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 10 AS claim_id,
                     service_date                                          AS claim_dt,
                     NULL                                                  AS claim_type,
                     NULL                                                  AS claim_bill_type,
                     NULL                                                  AS duration,
                     net_payment                                           AS pay_amt,
                     0                                                     AS medcr_amt,
                     total_charges                                         AS bill_amt,
                     0                                                     AS inst,
                     1                                                     AS prof,
                     0                                                     AS nurs,
                     1                                                     AS mc,
                     0                                                     AS ffs,
                     0                                                     AS crossover,
                     license_state ||
                     license_number ||
                     provider_license_type                                 AS provider_id,
                     CASE WHEN place_of_service = '23' THEN 1
                          WHEN place_of_service = '41' THEN 1
                          ELSE 0 END                                       AS plc_svc_ed
                FROM %ed_mc_prof%
               WHERE recipient_unique_id > 0

               UNION ALL

              --- Source #11: ED_RHP_PROF

              SELECT recipient_unique_id                                   AS re_unique_id,
                     recipient_unique_id*100000000000 + record_id*100 + 11 AS claim_id,
                     service_date                                          AS claim_dt,
                     NULL                                                  AS claim_type,
                     NULL                                                  AS claim_bill_type,
                     NULL                                                  AS duration,
                     net_payment                                           AS pay_amt,
                     0                                                     AS medcr_amt,
                     total_charges                                         AS bill_amt,
                     0                                                     AS inst,
                     1                                                     AS prof,
                     0                                                     AS nurs,
                     1                                                     AS mc,
                     0                                                     AS ffs,
                     0                                                     AS crossover,
                     license_state ||
                     license_number ||
                     provider_license_type                                 AS provider_id,
                     CASE WHEN place_of_service = '23' THEN 1
                          WHEN place_of_service = '41' THEN 1
                          ELSE 0 END                                       AS plc_svc_ed
                FROM %ed_rhp_prof%
               WHERE recipient_unique_id > 0

             ) claims
   LEFT JOIN %recip_x_ssn% rxn
          ON claims.re_unique_id = rxn.recipient_id
   LEFT JOIN (
              SELECT DISTINCT
                     claim_id,
                     1 AS "ED"
                FROM %rev_cde%
               WHERE SUBSTR(rev_cde, 0, 3) = '045' OR
                     rev_cde = '0981'
             ) rev_cde
          ON claims.claim_id = rev_cde.claim_id
   LEFT JOIN (
              SELECT DISTINCT
                     claim_id,
                     1 AS "ED"
                FROM %proc_cde%
               WHERE proc_cde IN ('99281', '99282', '99283', '99284', '99285', '99291', '99292')
             ) proc_cde
          ON claims.claim_id = proc_cde.claim_id
      """

with Connection() as cxn:
    cxn.clear_tables(table)
    cxn.execute(sql)
    cxn.save_table(table, "CLAIM_ID")

# vim: expandtab sw=4 ts=4
