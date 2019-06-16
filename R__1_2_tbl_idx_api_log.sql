-------------------------------------------------------------------------------
-- Table: IDX_API_LOG
-------------------------------------------------------------------------------
DECLARE
   v_cnt    NUMBER;
BEGIN
   select count(*) into v_cnt 
      from user_tables 
      where table_name='IDX_API_LOG';

   if v_cnt>0 then
      execute immediate 'DROP TABLE idx_api_log PURGE';
      execute immediate 'DROP SEQUENCE idx_api_log_seq';
   end if;
END;
/

CREATE SEQUENCE idx_api_log_seq START WITH 1 INCREMENT BY 1 CACHE 100;

CREATE TABLE idx_api_log (
   id             NUMBER(18,0) DEFAULT idx_api_log_seq.nextval,
   log_message    CLOB,
   status         VARCHAR2(1),
   error_code     NUMBER,
   error_msg      VARCHAR2(2000),
   created_ts     TIMESTAMP    DEFAULT systimestamp
);
-- Primary Key
CREATE UNIQUE INDEX idx_api_log_pk ON idx_api_log(id);
ALTER TABLE idx_api_log ADD CONSTRAINT idx_api_log_pk PRIMARY KEY (id) USING INDEX idx_api_log_pk;
-- Other constraints
ALTER TABLE idx_api_log ADD CONSTRAINT idx_api_log_status_ck CHECK (status IN ('S','E'));
