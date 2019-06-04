col l_table_name for a30
col l_index_name for a30
col l_column_list for a30
col e_table_name for a30
col e_index_name for a30
col e_column_list for a30
col action for a10
set pages 999

set serveroutput on size 1000000
DECLARE
/**********************************************************
 * Script: flyway_index_management.sql
 * Description:
 * ------------
 * This script will maintain the indexes for the domain
 * data schema. It will both create new indexes introduced
 * in the script, and delete old indexes which is removed
 * from this scripts. 
 **********************************************************/

-----------------------------------------------------------
-- Note! Set Index to be created below
-----------------------------------------------------------
   g_idx_json        CLOB                       := '
{
   "tables": [
      {
         "table_name": "EMP", 
               "indexes": [
                     {
                        "index_name": "emp_pk",
                        "column_list": "empno",
                        "is_unique": "Y"
                     },
                     {
                        "index_name": "emp_ename_idx",
                        "column_list": "ename",
                        "is_unique": "N"
                     },
                     {
                        "index_name": "emp_deptno_fk",
                        "column_list": "deptno",
                        "is_unique": "N"
                     },
                     {
                        "index_name": "emp_ename_job_idx",
                        "column_list": "ename, job",
                        "is_unique": "N"
                     },
                     {
                        "index_name": "emp_job_mgr_idx",
                        "column_list": "job, mgr",
                        "is_unique": "N"
                     },
                     {
                        "index_name": "emp_ename_empno_fidx",
                        "column_list": "upper(ename),empno",
                        "is_unique": "N"
                     },
                     {
                        "index_name": "emp_ename_deptno_idx",
                        "column_list": "ename, deptno",
                        "is_unique": "Y"
                     }
               ]
      },
      {
         "table_name": "DEPT", 
               "indexes": [
                     {
                        "index_name": "dept_pk",
                        "column_list": "deptno",
                        "is_unique": "Y"
                     },
                     {
                        "index_name": "dept_dname_deptno_idx",
                        "column_list": "dname,deptno",
                        "is_unique": "N"
                     }
            ]
      },
      {
         "table_name": "LOGGER_LOGS", 
               "indexes": [
                     {
                        "index_name": "LOGGER_LOGS_PK",
                        "column_list": "id",
                        "is_unique": "Y"
                     },
                     {
                        "index_name": "LOGGER_LOGS_IDX1",
                        "column_list": "TIME_STAMP,LOGGER_LEVEL",
                        "is_unique": "N"
                     }
            ]
      },
      {
         "table_name": "LOGGER_PREFS", 
               "indexes": [
                     {
                        "index_name": "LOGGER_PREFS_PK",
                        "column_list": "PREF_TYPE,PREF_NAME",
                        "is_unique": "Y"
                     }
            ]
      },
      {
         "table_name": "LOGGER_PREFS_BY_CLIENT_ID", 
               "indexes": [
                     {
                        "index_name": "LOGGER_PREFS_BY_CLIENT_ID_PK",
                        "column_list": "client_id",
                        "is_unique": "Y"
                     }
            ]
      },
      {
         "table_name": "LOGGER_LOGS_APEX_ITEMS", 
               "indexes": [
                     {
                        "index_name": "LOGGER_APEX_ITEMS_IDX1",
                        "column_list": "log_id",
                        "is_unique": "N"
                     },
                     {
                        "index_name": "LOGGER_LOGS_APX_ITMS_PK",
                        "column_list": "id",
                        "is_unique": "Y"
                     }
            ]
      }
   ]
}';

--===========================================================================--
-- NOTE! DO NOT EDIT BELOW THIS LINE
--===========================================================================--

   -----------------------------------------------------------
   -- Global Variables (g_xxxxx)
   -----------------------------------------------------------
   g_idx_list        flyway_index_adm_nt := flyway_index_adm_nt();
   
   -- for debug
   g_return          adm_idx.ResultSetCursor;
-------------------------------------------------------------------------------
-- MAIN
-------------------------------------------------------------------------------
BEGIN
   -----------------------------------------------------------
   -- Parse Json
   -----------------------------------------------------------
   g_idx_list := adm_idx.parse_index_json(g_idx_json);
   
   -----------------------------------------------------------
   -- Debug
   -----------------------------------------------------------   
   g_return := adm_idx.get_indexes_refcur(g_idx_list);
   DBMS_SQL.RETURN_RESULT(g_return);
   -----------------------------------------------------------
   -- Refactoring Indexe based on config above
   ----------------------------------------------------------- 
   adm_idx.refactor(g_idx_list);

-----------------------------------------------------------
-- Exception Handling
-----------------------------------------------------------
   exception
      when others then
         --dbms_output.put_line('SQL: ' || g_sql);
         raise;
END;
/