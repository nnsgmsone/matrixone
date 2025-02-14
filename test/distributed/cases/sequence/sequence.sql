drop sequence if exists s1;
create sequence s1 as smallint increment by -10 minvalue 30 maxvalue 100 cycle; 
select * from s1;
drop sequence if exists s1;
create sequence s1 as bigint unsigned increment by -1000 maxvalue 300;
select * from s1;
drop table s1;
show sequences;
create sequence `序列`;
select nextval('序列');
select nextval('s1'), currval('s1');
create sequence s2;
create table t1(a int);
insert into t1 values(nextval('s2'));
select * from t1;
drop sequence s5;
prepare stmt1 from 'insert into t1 values(?)';
-- @bvt:issue#9241
set @a_var = nextval('s2');
execute stmt1 using @a_var;
select * from t1;
-- @bvt:issue