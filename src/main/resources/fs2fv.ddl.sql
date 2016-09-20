drop table if exists dq_job_row_error;
drop table if exists dq_job_row_error_id_seq;
drop table if exists dq_job_file_error;
drop table if exists dq_job_file_error_id_seq;
drop table if exists dq_job_file;
drop table if exists dq_job_file_id_seq;
drop table if exists dq_job;
drop table if exists dq_job_id_seq;

/* dq_job contains a single row per validation job */
create table dq_job (
  id serial primary key,
  started_at timestamp not null
);

/* dq_job_file contains a single row per file, per validation job */
create table dq_job_file (
  id serial primary key,
  job_id integer references dq_job(id),
  file_id varchar(50)
);

/* records file-level errors (e.g. missing file) */
create table dq_job_file_error (
  id serial primary key,
  job_file_id integer references dq_job_file(id),
  message varchar(100) not null
);

/* records row-level errors (e.g. wrong-number of tokens) */
create table dq_job_row_error (
  id serial primary key,
  job_file_id integer references dq_job_file(id),
  row_num integer not null,
  message varchar(100) not null
)
