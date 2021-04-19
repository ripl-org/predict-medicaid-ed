import sys
from riipl import Connection

table = sys.argv[1]

sql = """
      CREATE TABLE %table% PCTFREE 0 NOLOGGING PARALLEL AS
      SELECT                                                         date_dt,
             CAST(TO_CHAR(date_dt, 'YYYYMM') AS NUMBER(6))        AS yrmo,
             TRUNC(date_dt, 'Q')                                  AS qtr_st_dt,
             CASE WHEN TO_NUMBER(TO_CHAR(date_dt, 'YYYY')) > 1917
                  THEN TO_CHAR(date_dt, 'YY') || 
                       TO_CHAR(date_dt, 'Q')
                  ELSE NULL
             END                                                  AS yyq
        FROM (
              SELECT TO_DATE('31-DEC-1799') + level AS date_dt
                FROM dual
          CONNECT BY level <= (SELECT TO_DATE('01-JAN-2101') - TO_DATE('01-JAN-1800') FROM dual)
             )
      """

with Connection() as cxn:
    cxn.clear_tables(table)
    cxn.execute(sql)
    cxn.save_table(table, "DATE_DT")

# vim: expandtab sw=4 ts=4
