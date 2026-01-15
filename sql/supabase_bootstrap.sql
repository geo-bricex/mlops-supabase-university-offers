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
