/***************************************************************
 * Script: cre_pkg_idx_api_vX.X.sql
 * Description:
 * ************
 * This script will create objects for index administration.
 * The following objects will be created:
 * - TYPE: IDX_API_OT (Object Type)
 * - TYPE: IDX_API_CT (Collection Type)
 * - PACKAGE: IDX_API
 ***************************************************************/
-- drop package adm_idx;
-- drop type idx_api_ct;
-- drop type idx_api_ot;

CREATE OR REPLACE TYPE idx_api_ot FORCE AS OBJECT (
   table_name     varchar2(128),
   index_name     varchar2(128),
   column_list    varchar2(1000),
   is_unique      varchar2(1),
   action         varchar2(20)
)
/

CREATE OR REPLACE TYPE idx_api_ct AS TABLE OF idx_api_ot
/

--grant execute on idx_api_ot to public;
--grant execute on idx_api_ct to public;

CREATE OR REPLACE PACKAGE idx_api AS

   ----------------------------------
   -- For Debug
   ----------------------------------   
   type ResultSetCursor is ref cursor;
   function get_indexes_refcur(idx_list_i IN idx_api_ct)  return ResultSetCursor; 
   
   ----------------------------------
   -- Constants
   ----------------------------------
   ERR_MSG_HEADER constant varchar2(50) := 'CREATE_INDEX_ERROR: ';
   
   ----------------------------------
   -- Public Procedures/ Functions
   ----------------------------------
   procedure add(tbl_i IN varchar2, 
                 idx_i IN varchar2, 
                 cols_i IN varchar2, 
                 unique_i IN varchar2 default 'N',
                 list_io IN OUT idx_api_ct);

   function analyze_idx_sql(list_i IN idx_api_ct) RETURN idx_api_ct PIPELINED;
   
   function parse_index_json(idx_json_i IN clob) return idx_api_ct;
   
   procedure refactor(list_i IN idx_api_ct);
END idx_api;
/

CREATE OR REPLACE PACKAGE BODY idx_api AS
   -- **********************************************************
   -- For Debug
   -- **********************************************************
   
   
   -- **********************************************************
   -- PRIVATE procedures
   -- **********************************************************
   procedure createIndex(tbl_i IN varchar2, idx_i IN varchar2, cols_i IN varchar2, unique_i IN varchar2) as
      l_sql varchar2(1000);
   begin
      l_sql := 'create ' || case when unique_i='Y' then 'unique ' else '' end ||
               'index ' || USER || '.' || idx_i || ' on ' || USER || '.' || tbl_i || '(' || cols_i || ')';
      execute immediate l_sql;
      dbms_output.put_line('SUCCESS: ' || l_sql);
   exception when others then
      dbms_output.put_line('ERROR in SQL: ' || l_sql);
      raise;
   end;
   
   procedure dropIndex(idx_i IN varchar2) as
      l_sql varchar2(1000);
   begin
      l_sql := 'drop index ' || USER || '.' || idx_i;
      execute immediate l_sql;
      dbms_output.put_line('SUCCESS: ' || l_sql);
   exception when others then
      dbms_output.put_line('ERROR in SQL: ' || l_sql);
      raise;
   end;
   
   procedure renameIndex(old_i IN varchar2, new_i IN varchar2) as
      l_sql varchar2(1000);
   begin
      l_sql := 'alter index ' || USER || '.' || old_i || ' rename to ' || new_i;
      execute immediate l_sql;
      dbms_output.put_line('SUCCESS: ' || l_sql);
   exception when others then
      dbms_output.put_line('ERROR in SQL: ' || l_sql);
      raise;
   end;

   procedure debug(txt_i IN varchar2) as
   begin
      dbms_output.put_line('DEBUG: ' || txt_i);
   end debug;

   -------------------------------------------------------------
   -- Procedure:   VALIDATE
   -- Description: Validate input (for instance duplicates).
   --
   -- Raises error when following occur:
   -- * Same column_list for same table
   -- * Non unique index name in schema
   -------------------------------------------------------------
   procedure validate(tbl_i IN varchar2, 
                     idx_i IN varchar2, 
                     cols_i IN varchar2, 
                     list_i IN idx_api_ct) as
   begin
      for r in (select * from table(list_i)) loop
         --------------------------------------------
         -- Check: Same COLUMN_LIST for same table
         --------------------------------------------
         if (r.table_name=tbl_i and r.column_list=cols_i) then
            raise_application_error( -20001, ERR_MSG_HEADER || 'Check duplicate column lists: ' ||
                                             upper(tbl_i) || '(' || cols_i || ')');
         --------------------------------------------
         -- Check: Unique indexname
         --------------------------------------------
         elsif (r.index_name=idx_i) then
            raise_application_error( -20001, ERR_MSG_HEADER || 'Check duplicate index names: ' ||
                                             upper(tbl_i) || '(' || cols_i || ')');
         end if;
      end loop;
   end validate;
   
   -------------------------------------------------------------
   -- Procedure:   VALIDATE_LIST
   -- Description: Validate input (for instance duplicates).
   --
   -- Raises error when following occur:
   -- * Same column_list for same table
   -- * Non unique index name in schema
   -------------------------------------------------------------
   procedure validate_list(list_i IN idx_api_ct) as
      l_cnt number;
   begin
      ----------------------------------------------
      -- Check for duplicate column lists
      ----------------------------------------------
      for r in (select *
                from (select table_name, column_list, count(*) cnt 
                      from table(list_i) group by table_name, column_list) t
                where cnt>1) loop
         raise_application_error( -20001, ERR_MSG_HEADER || 'Check duplicate column lists: ' ||
                                             upper(r.table_name) || '(' || r.column_list || ').');
      end loop;
      
      --------------------------------------------
      -- Check for duplicate index names
      --------------------------------------------
      for r in (select *
                from (select table_name, index_name, count(*) cnt 
                      from table(list_i) group by table_name, index_name) t
                where cnt>1) loop
         raise_application_error( -20001, ERR_MSG_HEADER || 'Check duplicate index names: ' ||
                                             upper(r.table_name) || '.' || r.index_name || '.');
      end loop;
   end validate_list;

   -------------------------------------------------------------
   -- Procedure:   VALIDATE_LIST
   -- Description: Validate input (for instance duplicates).
   --
   -- Raises error when following occur:
   -- * Same column_list for same table
   -- * Non unique index name in schema
   -------------------------------------------------------------
   procedure validate_table(tbl_name_i IN varchar2) as
      l_cnt number;
   begin
      ----------------------------------------------
      -- Check table exist
      ----------------------------------------------
      select count(*) into l_cnt
      from all_tables where owner=user and table_name=tbl_name_i;

      --------------------------------------------
      -- If not - RAISE ERROR
      --------------------------------------------
      if l_cnt=0 then
         raise_application_error( -20001, ERR_MSG_HEADER || 'Table does not exist: ' ||
                                             upper(tbl_name_i) || '.');
      end if;
   end validate_table;
   
   function analyze_idx_sql(list_i IN idx_api_ct) RETURN idx_api_ct PIPELINED AS
   begin
      for r in (
         with
               ind_expr_xml as (
                  select xmltype(dbms_xmlgen.getxml('select * from all_ind_expressions where index_owner='''||USER||'''')) as xml
                        from   dual
                        
               ),
               ind_expr as (
                  SELECT extractValue(xs.object_value, '/ROW/TABLE_NAME')       AS table_name
                   ,      extractValue(xs.object_value, '/ROW/INDEX_NAME')      AS index_name
                   ,      extractValue(xs.object_value, '/ROW/COLUMN_EXPRESSION')  AS column_expression
                  ,      extractValue(xs.object_value, '/ROW/COLUMN_POSITION') AS column_position
                  FROM   ind_expr_xml x, TABLE(XMLSEQUENCE(EXTRACT(x.xml, '/ROWSET/ROW'))) xs),
               ind_list_tmp as (
                  select c.table_name, c.index_name, c.column_name, c.column_position, 2 rk 
                  from all_ind_columns c where index_owner=USER
                  union all
                  select e.table_name, e.index_name, REGEXP_REPLACE( e.column_expression, '"', '' ), to_number(e.column_position), 1 rk 
                  from ind_expr e
               ),
               ind_list_tmp2 as (
                  select table_name, index_name, column_name, column_position
                  from (
                     select table_name, index_name, column_name, column_position, 
                           rank() over (partition by table_name, index_name, column_position order by rk) rn 
                     from ind_list_tmp)
                  where rn=1),
               ind_list as (
                  select table_name, index_name, 
                                       LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY column_position) column_list
                  from ind_list_tmp2
                  group by table_name, index_name)
               select nvl(l.table_name, e.table_name) table_name, 
                      nvl(l.index_name, e.index_name) index_name, 
                      nvl(l.column_list, e.column_list) column_list,
                      l.is_unique is_unique, 
                      case when e.table_name is null        then 'CREATE'
                           when l.table_name is null        then 'DROP'
                           when l.index_name<>e.index_name  then 'RENAME'
                           when (l.index_name=e.index_name 
                                 and l.column_list<>e.column_list)
                                                            then 'RECREATE' 
                                                            else 'EXIST' end action
               from table(list_i) l FULL JOIN ind_list e
                  ON (l.table_name=e.table_name
                        and (l.index_name=e.index_name or l.column_list=e.column_list))) 
      loop
          pipe row (idx_api_ot(r.table_name,
                                      r.index_name,
                                      r.column_list,
                                      r.is_unique,
                                      r.action) 
                   );
      end loop;
      return;
   end;
   -------------------------------------------------------------
   -- Procedure:   FBI_EXIST
   -- Description: Check if user owns function-based indexes
   -------------------------------------------------------------
   function fbi_exist return boolean is
      l_cnt    number;
   begin
      select count(*) into l_cnt from all_ind_expressions where index_owner=USER;
      
      return case when l_cnt=0 then false else true end;
   end;
   -------------------------------------------------------------
   -- Procedure:   GET_INDEXES_REFCUR
   -- Description: Function to return list of existing and
   --              non-existing indexes and what to do about it.
   --              Function used by refactor procedure and
   --              test script.
   -------------------------------------------------------------   
   function get_indexes_refcur(idx_list_i IN idx_api_ct)  return ResultSetCursor is
    resultSet ResultSetCursor;
   begin
      if fbi_exist() then
         open resultSet for 
            with
                  ind_expr_xml as (
                     select xmltype(dbms_xmlgen.getxml('select * from all_ind_expressions where index_owner='''||USER||'''')) as xml
                           from   dual
                           
                  ),
                  ind_expr as (
                     SELECT extractValue(xs.object_value, '/ROW/TABLE_NAME')       AS table_name
                      ,      extractValue(xs.object_value, '/ROW/INDEX_NAME')      AS index_name
                      ,      extractValue(xs.object_value, '/ROW/COLUMN_EXPRESSION')  AS column_expression
                     ,      extractValue(xs.object_value, '/ROW/COLUMN_POSITION') AS column_position
                     FROM   ind_expr_xml x, TABLE(XMLSEQUENCE(EXTRACT(x.xml, '/ROWSET/ROW'))) xs),
                  ind_list_tmp as (
                     select c.table_name, c.index_name, c.column_name, c.column_position, 2 rk 
                     from all_ind_columns c where index_owner=USER
                     union all
                     select e.table_name, e.index_name, REGEXP_REPLACE( e.column_expression, '"', '' ), to_number(e.column_position), 1 rk 
                     from ind_expr e
                  ),
                  ind_list_tmp2 as (
                     select table_name, index_name, column_name, column_position
                     from (
                        select table_name, index_name, column_name, column_position, 
                              rank() over (partition by table_name, index_name, column_position order by rk) rn 
                        from ind_list_tmp)
                     where rn=1),
                  ind_list_tmp3 as (
                     select table_name, index_name, 
                                          LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY column_position) column_list
                     from ind_list_tmp2
                     group by table_name, index_name),
                  ind_list as (
                     select t3.*, 
                            (select case when i.uniqueness='UNIQUE' then 'Y'
                                      when i.uniqueness='NONUNIQUE' then 'N'
                                      else null end
                              from all_indexes i
                              where owner=USER
                                and i.table_name=t3.table_name and i.index_name=t3.index_name)  is_unique
                     from ind_list_tmp3 t3
                  )
                  select l.table_name     l_table_name, 
                         l.index_name     l_index_name, 
                         l.column_list    l_column_list,
                         l.is_unique      l_is_unique, 
                         e.table_name     e_table_name,
                         e.index_name     e_index_name,
                         e.column_list    e_column_list,
                         e.is_unique      e_is_unique,
                         case when e.table_name is null        then 'CREATE'
                              when l.table_name is null        then 'DROP'
                              when l.index_name<>e.index_name
                               and l.table_name=e.table_name
                               and l.column_list=e.column_list 
                               and l.is_unique=e.is_unique     then 'RENAME'
                              when l.table_name=e.table_name
                                 and l.index_name=e.index_name
                                 and l.is_unique=e.is_unique   then 'EXIST'
                                                               else 'RECREATE' end action
                  from table(idx_list_i) l FULL JOIN ind_list e
                     ON (l.table_name=e.table_name
                           and (l.index_name=e.index_name or l.column_list=e.column_list))
                  order by nvl(l.table_name, e.table_name);
      else
         open resultSet for 
            with
                  ind_list_tmp as (
                     select c.table_name, c.index_name, c.column_name, c.column_position
                     from all_ind_columns c where index_owner=USER
                  ),
                  ind_list_tmp2 as (
                     select table_name, index_name, 
                                          LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY column_position) column_list
                     from ind_list_tmp
                     group by table_name, index_name),
                  ind_list as (
                     select t2.*, 
                            (select case when i.uniqueness='UNIQUE' then 'Y'
                                      when i.uniqueness='NONUNIQUE' then 'N'
                                      else null end
                              from all_indexes i
                              where owner=USER
                                and i.table_name=t2.table_name and i.index_name=t2.index_name)  is_unique
                     from ind_list_tmp2 t2
                  )
                  select l.table_name     l_table_name, 
                         l.index_name     l_index_name, 
                         l.column_list    l_column_list,
                         l.is_unique      l_is_unique, 
                         e.table_name     e_table_name,
                         e.index_name     e_index_name,
                         e.column_list    e_column_list,
                         e.is_unique      e_is_unique,
                         case when e.table_name is null        then 'CREATE'
                              when l.table_name is null        then 'DROP'
                              when l.index_name<>e.index_name
                               and l.table_name=e.table_name
                               and l.column_list=e.column_list 
                               and l.is_unique=e.is_unique     then 'RENAME'
                              when l.table_name=e.table_name
                                 and l.index_name=e.index_name
                                 and l.is_unique=e.is_unique   then 'EXIST'
                                                               else 'RECREATE' end action
                  from table(idx_list_i) l FULL JOIN ind_list e
                     ON (l.table_name=e.table_name
                           and (l.index_name=e.index_name or l.column_list=e.column_list))
                  order by nvl(l.table_name, e.table_name);
      end if;
      
      return resultSet;
   end get_indexes_refcur;
   
-- **************************************************************************--
-- PUBLIC procedures
-- **************************************************************************--
   ----------------------------------------------------------------------------
   -- Function: PARSE_JSON
   ----------------------------------------------------------------------------
   function parse_index_json(idx_json_i IN clob) return idx_api_ct as
      l_idx_list              idx_api_ct;

      l_tables_json           json_array_t;
      l_indexes_json          json_array_t;

      l_top_obj_json          json_object_t;
      l_table_json            json_object_t;
      l_index_json            json_object_t;
      
      l_table_name            varchar2(128);
      l_index_name            varchar2(128);
      l_index_cols            varchar2(128);
      l_index_uk              varchar2(128);

   begin
      -------------------------------------------------
      -- Initiate List of Indexes
      ------------------------------------------------- 
      l_idx_list   := idx_api_ct();

      -------------------------------------------------
      -- Parse Json Object
      -------------------------------------------------  
      l_top_obj_json := json_object_t(idx_json_i);

      -------------------------------------------------
      -- Parse Tables
      -------------------------------------------------     
      l_tables_json := l_top_obj_json.get_array('tables');

      FOR i IN 0..l_tables_json.get_size - 1 LOOP
         ----------------------------------
         -- Table: Parse & Validate
         ----------------------------------
         l_table_json := TREAT(l_tables_json.get(i) AS json_object_t); 
         l_table_name := upper(l_table_json.get_string('table_name'));
         
         validate_table(l_table_name);
         -------------------------------------------------
         -- Parse Indexes
         -------------------------------------------------     
         l_indexes_json := l_table_json.get_array('indexes');

         FOR j IN 0..l_indexes_json.get_size - 1 LOOP
            ----------------------------------
            -- Index: Parse & Add to list
            ----------------------------------
            l_index_json := TREAT(l_indexes_json.get(j) AS json_object_t);
            l_index_name := upper(l_index_json.get_string('index_name'));
            l_index_cols := upper(REGEXP_REPLACE(l_index_json.get_string('column_list'), '[[:space:]]', '' ));
            l_index_uk   := upper(l_index_json.get_string('is_unique'));
            
            l_idx_list.extend;
            l_idx_list(l_idx_list.count) := idx_api_ot(l_table_name, l_index_name, l_index_cols, l_index_uk, null);

         END LOOP;      -- Looping Indexes

      END LOOP;         -- Looping Tables

      return l_idx_list;
   end;
   -------------------------------------------------------------
   -- Procedure:   ADD
   -- Description: Add an index to the list to be maintained.
   -------------------------------------------------------------
   procedure add(tbl_i IN varchar2, 
                 idx_i IN varchar2, 
                 cols_i IN varchar2, 
                 unique_i IN varchar2 default 'N',
                 list_io IN OUT idx_api_ct) as
      l_cols varchar2(4000);
   begin
      ------------------------------------------
      -- Remove blanks in column list
      ------------------------------------------
      l_cols := REGEXP_REPLACE( cols_i, '[[:space:]]', '' );
      
      ------------------------------------------
      -- Add to list
      ------------------------------------------      
      list_io.extend;
      list_io(list_io.last) := idx_api_ot(upper(tbl_i), upper(idx_i), upper(l_cols), unique_i, null);
   end; 

   procedure refactor(list_i IN idx_api_ct) as
      l_rc sys_refcursor;
      
      l_tbl_name_list   varchar2(128);
      l_idx_name_list   varchar2(128);
      l_cols_list       varchar2(128);
      l_unique_list     varchar2(1);
      
      l_tbl_name_exist  varchar2(128);
      l_idx_name_exist  varchar2(128);
      l_cols_exist      varchar2(128);
      l_unique_exist    varchar2(1);
      
      l_action          varchar2(20);
      
   begin
      ------------------------------------------
      -- Validate
      ------------------------------------------      
      validate_list(list_i);
    
      ------------------------------------------
      -- Refactor
      ------------------------------------------
      l_rc := get_indexes_refcur(list_i); 
      loop
         fetch l_rc into l_tbl_name_list,  l_idx_name_list,  l_cols_list, l_unique_list, 
                         l_tbl_name_exist, l_idx_name_exist, l_cols_exist, l_unique_exist, l_action; 
         exit when l_rc%notfound;
         --debug('TBL: ' ||  rpad(case when r.e_table_name is null then r.l_table_name else r.e_table_name end, 30,' ') || 
         --      'LIST: ' || rpad(case when r.e_column_list is null then r.l_column_list else r.e_column_list end, 50,' ') || 
         --      ' EXIST: ' || rpad(nvl(r.e_column_list,' - '),20,' ') || ' ACTION: ' || r.rec_action);

         if (l_action='CREATE') then
            createIndex(l_tbl_name_list, l_idx_name_list, l_cols_list, l_unique_list);
         elsif (l_action='DROP') then
            dropIndex(l_idx_name_exist);
         elsif (l_action='RENAME') then
            renameIndex(l_idx_name_exist, l_idx_name_list);
         elsif (l_action='RECREATE') then
            dropIndex(l_idx_name_exist);
            createIndex(l_tbl_name_list, l_idx_name_list, l_cols_list, l_unique_list);
         end if;
         
      end loop;
   end;

END idx_api;
/


--grant execute on adm_idx to public;