-- Create/update a least-privilege app user for Module_5.
--
-- Usage (run as an admin/owner role):
--   psql -d grad_cafe \
--     -v app_user='grad_cafe_app' \
--     -v app_password='replace_with_strong_password' \
--     -f src/sql/create_least_privilege_app_user.sql
--
-- This script intentionally avoids owner/superuser privileges and only grants
-- what the app needs today: CONNECT, schema USAGE, SELECT + INSERT on
-- public.applicants, and sequence usage for SERIAL p_id inserts.

\set ON_ERROR_STOP 1

\if :{?app_user}
\else
\echo 'Missing required psql variable: app_user'
\quit 1
\endif

\if :{?app_password}
\else
\echo 'Missing required psql variable: app_password'
\quit 1
\endif

SELECT (to_regclass('public.applicants') IS NOT NULL) AS applicants_exists \gset
\if :applicants_exists
\else
\echo 'Table public.applicants does not exist. Create schema/table first.'
\quit 1
\endif

SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = :'app_user') AS role_exists \gset
\if :role_exists
SELECT format(
    'ALTER ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION',
    :'app_user',
    :'app_password'
) \gexec
\else
SELECT format(
    'CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION',
    :'app_user',
    :'app_password'
) \gexec
\endif

SELECT format(
    'GRANT CONNECT ON DATABASE %I TO %I',
    current_database(),
    :'app_user'
) \gexec
GRANT USAGE ON SCHEMA public TO :"app_user";

REVOKE ALL ON TABLE public.applicants FROM :"app_user";
GRANT SELECT, INSERT ON TABLE public.applicants TO :"app_user";

REVOKE ALL ON SEQUENCE public.applicants_p_id_seq FROM :"app_user";
GRANT USAGE, SELECT ON SEQUENCE public.applicants_p_id_seq TO :"app_user";

-- Remove inherited CREATE from PUBLIC and explicitly disallow CREATE for app role.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM :"app_user";
