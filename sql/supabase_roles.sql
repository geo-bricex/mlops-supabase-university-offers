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

    EXECUTE 'GRANT USAGE ON SCHEMA storage TO anon, authenticated, service_role';
    EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA storage TO service_role';
    EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA storage TO anon, authenticated';

    EXECUTE 'ALTER TABLE storage.buckets ENABLE ROW LEVEL SECURITY';
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = 'storage' AND tablename = 'buckets' AND policyname = 'service_role_all_buckets'
    ) THEN
        EXECUTE 'CREATE POLICY service_role_all_buckets ON storage.buckets FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;

    EXECUTE 'ALTER TABLE storage.objects ENABLE ROW LEVEL SECURITY';
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = 'storage' AND tablename = 'objects' AND policyname = 'service_role_all_objects'
    ) THEN
        EXECUTE 'CREATE POLICY service_role_all_objects ON storage.objects FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;

    EXECUTE 'ALTER TABLE storage.s3_multipart_uploads ENABLE ROW LEVEL SECURITY';
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = 'storage' AND tablename = 's3_multipart_uploads' AND policyname = 'service_role_all_multipart_uploads'
    ) THEN
        EXECUTE 'CREATE POLICY service_role_all_multipart_uploads ON storage.s3_multipart_uploads FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;

    EXECUTE 'ALTER TABLE storage.s3_multipart_uploads_parts ENABLE ROW LEVEL SECURITY';
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = 'storage' AND tablename = 's3_multipart_uploads_parts' AND policyname = 'service_role_all_multipart_parts'
    ) THEN
        EXECUTE 'CREATE POLICY service_role_all_multipart_parts ON storage.s3_multipart_uploads_parts FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END
$$;
