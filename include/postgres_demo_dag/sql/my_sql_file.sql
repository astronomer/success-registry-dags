drop table if exists public.my_sql_table;
create table if not exists public.my_sql_table (
  col_1 varchar,
  col_2 varchar,
  col_3 varchar
);

insert into public.my_sql_table (col_1, col_2, col_3)
values('hard-coded-value-1', 'hard-coded-value-2', 'hard-coded-value-3')
;

insert into public.my_sql_table (col_1, col_2, col_3)
values('hard-coded-value-4', 'hard-coded-value-5', 'hard-coded-value-6')
;

insert into public.my_sql_table (col_1, col_2, col_3)
values('hard-coded-value-7', 'hard-coded-value-8', 'hard-coded-value-9')
;

insert into public.my_sql_table (col_1, col_2, col_3)
values('{{ params.value_1 }}', '{{ params.value_2 }}', '{{ ds }}')
;

