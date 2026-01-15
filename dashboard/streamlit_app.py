import json
import os
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

try:
    from src.db.init_db import ensure_schema
except ImportError:
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from src.db.init_db import ensure_schema

# Page Config
st.set_page_config(page_title="Ecuador Academic Offer", layout="wide")


def normalize_text(value: str) -> str:
    if not isinstance(value, str):
        return ""
    text = unicodedata.normalize('NFKD', value).encode('ASCII', 'ignore').decode('utf-8')
    text = text.lower().strip()
    text = " ".join(text.split())
    return text


@st.cache_resource
def get_engine():
    # Use environment variable or fallback
    db_url = os.getenv(
        "DB_CONNECTION_STRING",
        "postgresql://supabase_admin:your-super-secret-and-long-postgres-password@db:5432/postgres"
    )
    return create_engine(db_url)


@st.cache_data
def load_geojson(path: str, name_key: str):
    geo = json.loads(Path(path).read_text(encoding="utf-8"))
    for feature in geo.get("features", []):
        props = feature.get("properties", {})
        name = props.get(name_key, "")
        props["name_norm"] = normalize_text(name)
        if "NAME_1" in props:
            props["province_norm"] = normalize_text(props.get("NAME_1", ""))
        if "NAME_2" in props:
            props["canton_norm"] = normalize_text(props.get("NAME_2", ""))
    return geo


def load_data(query, params=None):
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)


def parse_metrics(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


def parse_json_value(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


engine = get_engine()
if os.getenv("DB_AUTO_INIT", "true").lower() in ("1", "true", "yes"):
    try:
        ensure_schema(engine)
    except Exception as e:
        st.warning(f"Schema init failed: {e}")

# Base dataset
try:
    df = load_data("""
        SELECT
            f.offer_id,
            f.estado_norm,
            f.is_current,
            f.last_seen_at,
            rf.ingested_at,
            i.nombre_norm AS ies,
            i.tipo_ies,
            i.tipo_financiamiento,
            t.provincia_norm,
            t.canton_norm,
            p.carrera_norm,
            p.campo_amplio,
            p.nivel_formacion,
            p.modalidad
        FROM core.fact_offer f
        JOIN core.dim_ies i ON f.ies_id = i.ies_id
        JOIN core.dim_territory t ON f.territory_id = t.territory_id
        JOIN core.dim_program p ON f.program_id = p.program_id
        LEFT JOIN raw_ingest.files rf ON f.last_file_id = rf.file_id
        WHERE f.is_current = TRUE
    """)
except Exception as e:
    st.error(
        "Database connection failed or tables not ready. "
        "Run the schema init and ETL before opening the dashboard. "
        f"Error: {e}"
    )
    st.stop()

if df.empty:
    st.warning("No data loaded yet. Run the ETL to populate Supabase tables.")
    st.stop()

df["ingested_at"] = pd.to_datetime(df["ingested_at"], errors="coerce", utc=True)

# Sidebar Filters
st.sidebar.title("Filters")

province_options = sorted(df["provincia_norm"].dropna().unique())
selected_prov = st.sidebar.multiselect("Province", province_options)

if selected_prov:
    canton_options = sorted(df[df["provincia_norm"].isin(selected_prov)]["canton_norm"].dropna().unique())
else:
    canton_options = sorted(df["canton_norm"].dropna().unique())
selected_canton = st.sidebar.multiselect("Canton", canton_options)

campo_options = sorted(df["campo_amplio"].dropna().unique())
nivel_options = sorted(df["nivel_formacion"].dropna().unique())
modalidad_options = sorted(df["modalidad"].dropna().unique())
tipo_ies_options = sorted(df["tipo_ies"].dropna().unique())
tipo_fin_options = sorted(df["tipo_financiamiento"].dropna().unique())
estado_options = sorted(df["estado_norm"].dropna().unique())

selected_campo = st.sidebar.multiselect("Field of Study", campo_options)
selected_nivel = st.sidebar.multiselect("Education Level", nivel_options)
selected_modalidad = st.sidebar.multiselect("Modality", modalidad_options)
selected_tipo_ies = st.sidebar.multiselect("IES Type", tipo_ies_options)
selected_tipo_fin = st.sidebar.multiselect("Funding Type", tipo_fin_options)
selected_estado = st.sidebar.multiselect("Estado", estado_options)

date_min = df["ingested_at"].min()
date_max = df["ingested_at"].max()
if pd.notna(date_min) and pd.notna(date_max):
    date_range = st.sidebar.date_input(
        "Ingestion Date Range",
        value=(date_min.date(), date_max.date()),
        min_value=date_min.date(),
        max_value=date_max.date()
    )
else:
    date_range = None

filtered = df.copy()
if selected_prov:
    filtered = filtered[filtered["provincia_norm"].isin(selected_prov)]
if selected_canton:
    filtered = filtered[filtered["canton_norm"].isin(selected_canton)]
if selected_campo:
    filtered = filtered[filtered["campo_amplio"].isin(selected_campo)]
if selected_nivel:
    filtered = filtered[filtered["nivel_formacion"].isin(selected_nivel)]
if selected_modalidad:
    filtered = filtered[filtered["modalidad"].isin(selected_modalidad)]
if selected_tipo_ies:
    filtered = filtered[filtered["tipo_ies"].isin(selected_tipo_ies)]
if selected_tipo_fin:
    filtered = filtered[filtered["tipo_financiamiento"].isin(selected_tipo_fin)]
if selected_estado:
    filtered = filtered[filtered["estado_norm"].isin(selected_estado)]
if date_range and len(date_range) == 2:
    start_date, end_date = date_range
    start_ts = pd.Timestamp(start_date, tz="UTC")
    end_ts = pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1)
    filtered = filtered[
        (filtered["ingested_at"] >= start_ts) &
        (filtered["ingested_at"] < end_ts)
    ]

if filtered.empty:
    st.warning("No records match the selected filters.")
has_data = not filtered.empty

tab_overview, tab_geo, tab_diversity, tab_quality, tab_timeline, tab_monitoring = st.tabs(
    ["Overview", "Geographic Coverage", "Diversity & Institutions", "Data Quality", "Timeline", "Monitoring"]
)

with tab_overview:
    st.header("KPIs")
    if has_data:
        kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
        kpi1.metric("Total Active Offers", len(filtered))
        kpi2.metric("Unique IES", filtered["ies"].nunique())
        program_count = filtered[["carrera_norm", "campo_amplio", "nivel_formacion", "modalidad"]].drop_duplicates().shape[0]
        kpi3.metric("Programs", program_count)
        kpi4.metric("Provinces Covered", filtered["provincia_norm"].nunique())
        kpi5.metric("Cantons Covered", filtered["canton_norm"].nunique())

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Offers by Estado")
            fig_state = px.pie(filtered, names='estado_norm', title='Offer Status Distribution')
            st.plotly_chart(fig_state, use_container_width=True)

        with col2:
            st.subheader("Offers by Modality")
            fig_mod = px.bar(filtered['modalidad'].value_counts(), orientation='h', title='Modality Count')
            st.plotly_chart(fig_mod, use_container_width=True)

        st.subheader("Top IES by Offer Volume")
        top_ies = filtered["ies"].value_counts().head(10).rename_axis("ies").reset_index(name="offers")
        st.dataframe(top_ies)
    else:
        st.info("No data available for the selected filters.")

with tab_geo:
    st.header("Territorial Coverage")
    if not has_data:
        st.info("No data available for the selected filters.")
    else:
        prov_counts = filtered.groupby('provincia_norm').size().reset_index(name='offers')
        prov_div = filtered.groupby('provincia_norm')['campo_amplio'].nunique().reset_index(name='field_diversity')
        prov_counts = prov_counts.merge(prov_div, on='provincia_norm', how='left')

        prov_geo_path = "assets/geo/ecuador_provinces.geojson"
        canton_geo_path = "assets/geo/ecuador_cantons.geojson"
        if Path(prov_geo_path).exists():
            prov_geo = load_geojson(prov_geo_path, "NAME_1")
            fig_prov = px.choropleth(
                prov_counts,
                geojson=prov_geo,
                locations="provincia_norm",
                color="offers",
                featureidkey="properties.name_norm",
                hover_data={"field_diversity": True},
                color_continuous_scale="Blues",
                title="Active Offers by Province"
            )
            fig_prov.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig_prov, use_container_width=True)
        else:
            st.info("Province GeoJSON not found. Add assets/geo/ecuador_provinces.geojson.")

        st.subheader("Canton Drill-down")
        if province_options:
            selected_map_prov = st.selectbox("Province for canton view", province_options)
            canton_counts = filtered[filtered["provincia_norm"] == selected_map_prov].groupby("canton_norm").size().reset_index(name="offers")

            if Path(canton_geo_path).exists():
                canton_geo = load_geojson(canton_geo_path, "NAME_2")
                canton_geo["features"] = [
                    f for f in canton_geo.get("features", [])
                    if f.get("properties", {}).get("province_norm") == selected_map_prov
                ]
                fig_canton = px.choropleth(
                    canton_counts,
                    geojson=canton_geo,
                    locations="canton_norm",
                    color="offers",
                    featureidkey="properties.canton_norm",
                    color_continuous_scale="Viridis",
                    title=f"Active Offers by Canton - {selected_map_prov.title()}"
                )
                fig_canton.update_geos(fitbounds="locations", visible=False)
                st.plotly_chart(fig_canton, use_container_width=True)
            else:
                st.info("Canton GeoJSON not found. Add assets/geo/ecuador_cantons.geojson.")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Top Provinces by Offer Volume")
            st.dataframe(prov_counts.sort_values("offers", ascending=False).head(10))
        with col2:
            st.subheader("Top Cantons by Offer Volume")
            canton_top = filtered.groupby("canton_norm").size().reset_index(name="offers").sort_values("offers", ascending=False)
            st.dataframe(canton_top.head(10))

with tab_diversity:
    st.header("Diversity & Concentration")
    if not has_data:
        st.info("No data available for the selected filters.")
    else:
        def hhi(series):
            shares = series.value_counts(normalize=True)
            return (shares ** 2).sum()

        def entropy(series):
            shares = series.value_counts(normalize=True)
            return -(shares * shares.apply(lambda x: 0 if x == 0 else np.log2(x))).sum()

        prov_hhi = filtered.groupby("provincia_norm")["campo_amplio"].apply(hhi).reset_index(name="hhi")
        prov_entropy = filtered.groupby("provincia_norm")["campo_amplio"].apply(entropy).reset_index(name="entropy")
        prov_diversity = prov_hhi.merge(prov_entropy, on="provincia_norm", how="left")

        st.subheader("Most Specialized Provinces (High HHI)")
        st.dataframe(prov_diversity.sort_values("hhi", ascending=False).head(10))

        st.subheader("Most Diversified Provinces (Low HHI)")
        st.dataframe(prov_diversity.sort_values("hhi", ascending=True).head(10))

        canton_hhi = filtered.groupby("canton_norm")["campo_amplio"].apply(hhi).reset_index(name="hhi")
        canton_entropy = filtered.groupby("canton_norm")["campo_amplio"].apply(entropy).reset_index(name="entropy")
        canton_diversity = canton_hhi.merge(canton_entropy, on="canton_norm", how="left")

        st.subheader("Most Specialized Cantons (High HHI)")
        st.dataframe(canton_diversity.sort_values("hhi", ascending=False).head(10))

        st.subheader("Most Diversified Cantons (Low HHI)")
        st.dataframe(canton_diversity.sort_values("hhi", ascending=True).head(10))

        st.subheader("Institution Profiling")
        ies_counts = filtered.groupby("ies").size().reset_index(name="offers").sort_values("offers", ascending=False)
        ies_fields = filtered.groupby("ies")["campo_amplio"].nunique().reset_index(name="unique_fields")
        ies_levels = filtered.groupby("ies")["nivel_formacion"].nunique().reset_index(name="unique_levels")
        ies_profile = ies_counts.merge(ies_fields, on="ies", how="left").merge(ies_levels, on="ies", how="left")
        st.dataframe(ies_profile.head(15))

with tab_quality:
    st.header("Data Quality")

    runs = load_data("""
        SELECT run_id, file_id, created_at, metrics
        FROM audit.data_quality_runs
        ORDER BY created_at DESC
    """)
    if runs.empty:
        st.info("No data quality runs found yet.")
    else:
        runs["metrics_dict"] = runs["metrics"].apply(parse_metrics)
        metrics_df = pd.json_normalize(runs["metrics_dict"])
        metrics_df["created_at"] = runs["created_at"]

        st.subheader("Latest Run Metrics")
        st.json(runs.iloc[0]["metrics_dict"])

        metric_cols = [c for c in metrics_df.columns if c != "created_at"]
        selected_metrics = st.multiselect(
            "Metrics to plot",
            metric_cols,
            default=[c for c in metric_cols if "invalid" in c or "duplicate" in c]
        )
        if selected_metrics:
            plot_df = metrics_df[["created_at"] + selected_metrics].sort_values("created_at")
            fig_metrics = px.line(plot_df, x="created_at", y=selected_metrics, markers=True, title="DQ Metrics Over Time")
            st.plotly_chart(fig_metrics, use_container_width=True)

    issues = load_data("""
        SELECT issue_id, run_id, created_at, issue_type, natural_key, detail
        FROM audit.inconsistencies
        ORDER BY created_at DESC
        LIMIT 2000
    """)
    if issues.empty:
        st.info("No inconsistencies found.")
    else:
        issues["detail"] = issues["detail"].apply(lambda d: json.dumps(d) if isinstance(d, dict) else d)
        issue_types = sorted(issues["issue_type"].dropna().unique())
        selected_issues = st.multiselect("Issue Types", issue_types, default=issue_types)
        filtered_issues = issues[issues["issue_type"].isin(selected_issues)]
        st.dataframe(filtered_issues)

        csv_bytes = filtered_issues.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download Inconsistencies CSV",
            data=csv_bytes,
            file_name="inconsistencies.csv",
            mime="text/csv"
        )

with tab_timeline:
    st.header("Ingestion Timeline")

    files = load_data("""
        SELECT
            file_id,
            file_name,
            rows_loaded,
            ingested_at,
            status,
            notes,
            started_at,
            finished_at,
            duration_seconds,
            file_size_bytes,
            ingest_new,
            ingest_updated,
            ingest_unchanged,
            skipped_missing_dims,
            storage_status,
            storage_paths
        FROM raw_ingest.files
        ORDER BY ingested_at DESC
    """)
    st.subheader("Ingestion Runs")
    st.dataframe(files)

    if not files.empty:
        files["storage_paths_parsed"] = files["storage_paths"].apply(parse_json_value)
        latest = files.iloc[0]
        if latest.get("storage_paths_parsed"):
            st.subheader("Latest Storage Artifacts")
            st.json(latest["storage_paths_parsed"])

        if "duration_seconds" in files.columns and files["duration_seconds"].notna().any():
            st.subheader("Pipeline Duration (seconds)")
            duration_series = files.sort_values("ingested_at")
            fig_duration = px.line(
                duration_series,
                x="ingested_at",
                y="duration_seconds",
                markers=True,
                title="ETL Duration Over Time"
            )
            st.plotly_chart(fig_duration, use_container_width=True)

        if "file_size_bytes" in files.columns and files["file_size_bytes"].notna().any():
            st.subheader("File Size (MB)")
            size_series = files.sort_values("ingested_at")
            size_series["file_size_mb"] = size_series["file_size_bytes"] / (1024 * 1024)
            fig_size = px.line(
                size_series,
                x="ingested_at",
                y="file_size_mb",
                markers=True,
                title="Source File Size Over Time (MB)"
            )
            st.plotly_chart(fig_size, use_container_width=True)

        if "storage_status" in files.columns and files["storage_status"].notna().any():
            st.subheader("Storage Upload Status")
            status_counts = files["storage_status"].value_counts().reset_index()
            status_counts.columns = ["status", "count"]
            fig_status = px.bar(status_counts, x="status", y="count", title="Storage Status Counts")
            st.plotly_chart(fig_status, use_container_width=True)

        runs = load_data("""
            SELECT file_id, created_at, metrics
            FROM audit.data_quality_runs
            ORDER BY created_at ASC
        """)
        if not runs.empty:
            runs["metrics_dict"] = runs["metrics"].apply(parse_metrics)
            metrics = pd.json_normalize(runs["metrics_dict"])
            metrics["file_id"] = runs["file_id"]
            timeline = files.merge(metrics, on="file_id", how="left", suffixes=("", "_metric"))
            timeline = timeline.sort_values("ingested_at")

            base_cols = ["rows_loaded", "ingest_new", "ingest_updated", "ingest_unchanged", "skipped_missing_dims"]
            for col in base_cols:
                metric_col = f"{col}_metric"
                if metric_col in timeline.columns:
                    timeline[col] = pd.to_numeric(timeline[col], errors="coerce")
                    timeline[metric_col] = pd.to_numeric(timeline[metric_col], errors="coerce")
                    timeline[col] = timeline[col].fillna(timeline[metric_col])

            cols = [c for c in base_cols if c in timeline.columns]
            if cols:
                timeline_plot = timeline[["ingested_at"] + cols].copy()
                for col in cols:
                    timeline_plot[col] = pd.to_numeric(timeline_plot[col], errors="coerce")
                timeline_plot = timeline_plot.melt(
                    id_vars="ingested_at",
                    var_name="metric",
                    value_name="value"
                ).dropna(subset=["value"])
                if not timeline_plot.empty:
                    fig_timeline = px.line(
                        timeline_plot,
                        x="ingested_at",
                        y="value",
                        color="metric",
                        markers=True,
                        title="Rows and Change Counts by Ingestion"
                    )
                    st.plotly_chart(fig_timeline, use_container_width=True)

        state_hist = load_data("""
            SELECT f.file_id, f.ingested_at, s.estado AS estado, COUNT(*) AS count
            FROM raw_ingest.stg_oferta s
            JOIN raw_ingest.files f ON s.file_id = f.file_id
            GROUP BY f.file_id, f.ingested_at, s.estado
            ORDER BY f.ingested_at
        """)
        if not state_hist.empty:
            fig_state = px.area(
                state_hist,
                x="ingested_at",
                y="count",
                color="estado",
                title="Estado Distribution Over Time (by file)"
            )
            st.plotly_chart(fig_state, use_container_width=True)

with tab_monitoring:
    st.header("Monitoring")

    files = load_data("""
        SELECT
            file_id,
            file_name,
            rows_loaded,
            ingested_at,
            status,
            started_at,
            finished_at,
            duration_seconds,
            file_size_bytes,
            ingest_new,
            ingest_updated,
            ingest_unchanged,
            storage_status,
            storage_paths,
            process_metrics
        FROM raw_ingest.files
        ORDER BY ingested_at DESC
    """)

    st.subheader("Pipeline Health")
    if files.empty:
        st.info("No ingestion runs found yet.")
    else:
        files["ingested_at"] = pd.to_datetime(files["ingested_at"], errors="coerce")
        success_rate = (files["status"] == "success").mean() * 100
        durations = files["duration_seconds"].dropna()
        avg_duration = durations.mean() if not durations.empty else None
        throughput = files.dropna(subset=["rows_loaded", "duration_seconds"]).copy()
        throughput = throughput[throughput["duration_seconds"] > 0]
        avg_rps = (throughput["rows_loaded"] / throughput["duration_seconds"]).mean() if not throughput.empty else None

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Runs", len(files))
        c2.metric("Success Rate", f"{success_rate:.1f}%")
        c3.metric("Avg Duration (s)", f"{avg_duration:.1f}" if avg_duration else "n/a")
        c4.metric("Avg Rows/sec", f"{avg_rps:.1f}" if avg_rps else "n/a")

        status_counts = files["status"].value_counts().reset_index()
        status_counts.columns = ["status", "count"]
        fig_status = px.bar(status_counts, x="status", y="count", title="Run Status Counts")
        st.plotly_chart(fig_status, use_container_width=True)

    st.subheader("Latest Process Metrics")
    if not files.empty and "process_metrics" in files.columns:
        latest_metrics = parse_json_value(files.iloc[0].get("process_metrics"))
        if latest_metrics:
            st.json(latest_metrics)
        else:
            st.info("No process metrics available for the latest run.")
    else:
        st.info("No process metrics available yet.")

    st.subheader("ETL Step Metrics")
    try:
        steps = load_data("""
            SELECT file_id, step_name, started_at, duration_seconds, row_count, detail
            FROM ops.etl_step_metrics
            ORDER BY started_at DESC
            LIMIT 2000
        """)
    except Exception:
        steps = pd.DataFrame()

    if steps.empty:
        st.info("No step metrics yet. Run the ETL to populate them.")
    else:
        steps["duration_seconds"] = pd.to_numeric(steps["duration_seconds"], errors="coerce")
        steps["row_count"] = pd.to_numeric(steps["row_count"], errors="coerce")
        steps["started_at"] = pd.to_datetime(steps["started_at"], errors="coerce")

        if not files.empty:
            latest_file_id = files.iloc[0]["file_id"]
            latest_steps = steps[steps["file_id"] == latest_file_id].sort_values("started_at")
        else:
            latest_steps = steps.sort_values("started_at").groupby("file_id").tail(1)

        if not latest_steps.empty:
            st.markdown("Latest run step timings:")
            st.dataframe(latest_steps[["step_name", "duration_seconds", "row_count", "started_at", "detail"]])
            fig_steps = px.bar(
                latest_steps,
                x="step_name",
                y="duration_seconds",
                title="Latest Run Step Durations"
            )
            st.plotly_chart(fig_steps, use_container_width=True)

        step_avg = steps.groupby("step_name", as_index=False)["duration_seconds"].mean()
        step_avg = step_avg.sort_values("duration_seconds", ascending=False)
        fig_avg = px.bar(step_avg, x="step_name", y="duration_seconds", title="Average Step Duration")
        st.plotly_chart(fig_avg, use_container_width=True)

    st.subheader("Service Health")
    try:
        health = load_data("""
            SELECT service_name, endpoint, status, status_code, latency_ms, created_at, detail
            FROM ops.service_health
            ORDER BY created_at DESC
            LIMIT 500
        """)
    except Exception:
        health = pd.DataFrame()
    if health.empty:
        st.info("No service checks yet. Run: python -m src.ops.monitor")
    else:
        health["created_at"] = pd.to_datetime(health["created_at"], errors="coerce")
        latest = health.sort_values("created_at", ascending=False).groupby("service_name").head(1)
        st.dataframe(latest[["service_name", "status", "status_code", "latency_ms", "created_at", "endpoint"]])

        counts = health.groupby(["service_name", "status"]).size().reset_index(name="count")
        fig_health = px.bar(counts, x="service_name", y="count", color="status", title="Service Health Checks")
        st.plotly_chart(fig_health, use_container_width=True)

    st.subheader("Storage Artifacts")
    if files.empty:
        st.info("No storage metadata available yet.")
    else:
        files["storage_paths_parsed"] = files["storage_paths"].apply(parse_json_value)
        latest_paths = files.iloc[0]["storage_paths_parsed"] if "storage_paths_parsed" in files else {}
        if latest_paths:
            st.markdown("Latest run artifacts:")
            for key, info in latest_paths.items():
                if isinstance(info, dict):
                    url = info.get("url")
                    path = info.get("path") or key
                    if url:
                        st.markdown(f"- {key}: [{path}]({url})")
                    else:
                        st.markdown(f"- {key}: {path}")

        artifact_rows = []
        for _, row in files.iterrows():
            paths = row.get("storage_paths_parsed") or {}
            for name, info in paths.items():
                if isinstance(info, dict):
                    artifact_rows.append({
                        "file_id": row.get("file_id"),
                        "artifact": name,
                        "path": info.get("path"),
                        "url": info.get("url"),
                    })
        if artifact_rows:
            artifact_df = pd.DataFrame(artifact_rows)
            st.dataframe(artifact_df, use_container_width=True)
        else:
            st.info("Storage uploads are not available yet. Check SUPABASE_SERVICE_ROLE_KEY and rerun ETL.")
