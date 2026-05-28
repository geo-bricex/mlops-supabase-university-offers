#!/bin/sh
set -eu

psql -v ON_ERROR_STOP=1 -U supabase_admin -d postgres -v password="$POSTGRES_PASSWORD" <<'SQL'
SET app.password = :'password';

DO $$
DECLARE
    pwd TEXT := current_setting('app.password', true);
BEGIN
    IF pwd IS NULL OR pwd = '' THEN
        RAISE EXCEPTION 'app.password is not set';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_auth_admin') THEN
        EXECUTE format('CREATE ROLE supabase_auth_admin LOGIN PASSWORD %L SUPERUSER', pwd);
    ELSE
        EXECUTE format('ALTER ROLE supabase_auth_admin WITH LOGIN PASSWORD %L SUPERUSER', pwd);
    END IF;
    EXECUTE 'ALTER ROLE supabase_auth_admin SET search_path = auth, public';

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'postgres') THEN
        EXECUTE format('CREATE ROLE postgres LOGIN PASSWORD %L SUPERUSER', pwd);
    ELSE
        EXECUTE format('ALTER ROLE postgres WITH LOGIN PASSWORD %L SUPERUSER', pwd);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_storage_admin') THEN
        EXECUTE format('CREATE ROLE supabase_storage_admin LOGIN PASSWORD %L SUPERUSER', pwd);
    ELSE
        EXECUTE format('ALTER ROLE supabase_storage_admin WITH LOGIN PASSWORD %L SUPERUSER', pwd);
    END IF;
    EXECUTE 'ALTER ROLE supabase_storage_admin SET search_path = storage, public';

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_realtime_admin') THEN
        EXECUTE format('CREATE ROLE supabase_realtime_admin LOGIN PASSWORD %L SUPERUSER', pwd);
    ELSE
        EXECUTE format('ALTER ROLE supabase_realtime_admin WITH LOGIN PASSWORD %L SUPERUSER', pwd);
    END IF;
    EXECUTE 'ALTER ROLE supabase_realtime_admin SET search_path = _realtime, public';

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticator') THEN
        EXECUTE format('CREATE ROLE authenticator LOGIN PASSWORD %L SUPERUSER', pwd);
    ELSE
        EXECUTE format('ALTER ROLE authenticator WITH LOGIN PASSWORD %L SUPERUSER', pwd);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
        EXECUTE 'CREATE ROLE anon NOLOGIN';
    END IF;
    EXECUTE 'ALTER ROLE anon SET search_path = storage, public';

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
        EXECUTE 'CREATE ROLE authenticated NOLOGIN';
    END IF;
    EXECUTE 'ALTER ROLE authenticated SET search_path = storage, public';

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
        EXECUTE 'CREATE ROLE service_role NOLOGIN';
    END IF;
    EXECUTE 'ALTER ROLE service_role SET search_path = storage, public';
END
$$;

CREATE SCHEMA IF NOT EXISTS storage;
ALTER SCHEMA storage OWNER TO supabase_storage_admin;

CREATE SCHEMA IF NOT EXISTS auth;
ALTER SCHEMA auth OWNER TO supabase_auth_admin;

CREATE SCHEMA IF NOT EXISTS _realtime;
ALTER SCHEMA _realtime OWNER TO supabase_realtime_admin;

CREATE SCHEMA IF NOT EXISTS graphql_public;
ALTER SCHEMA graphql_public OWNER TO supabase_admin;

CREATE SCHEMA IF NOT EXISTS storage;
ALTER SCHEMA storage OWNER TO supabase_storage_admin;

CREATE SCHEMA IF NOT EXISTS _analytics;
ALTER SCHEMA _analytics OWNER TO supabase_admin;

CREATE SCHEMA IF NOT EXISTS extensions;
ALTER SCHEMA extensions OWNER TO supabase_admin;
SQL
