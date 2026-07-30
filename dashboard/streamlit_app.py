import json
import os
import textwrap
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import requests
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


def prepare_dataframe_for_display(df: pd.DataFrame) -> pd.DataFrame:
    display_df = df.copy()
    for column in display_df.columns:
        if pd.api.types.is_object_dtype(display_df[column]) or pd.api.types.is_string_dtype(display_df[column]):
            display_df[column] = display_df[column].apply(
                lambda value: json.dumps(value, ensure_ascii=False)
                if isinstance(value, (dict, list))
                else str(value)
                if value is not None and not pd.isna(value)
                else value
            )
    return display_df


def show_dataframe(df: pd.DataFrame, **kwargs):
    st.dataframe(prepare_dataframe_for_display(df), **kwargs)


def get_ollama_settings():
    internal_url = os.getenv(
        "OLLAMA_INTERNAL_URL",
        os.getenv("OLLAMA_URL", "http://ollama:11434"),
    ).rstrip("/")
    return {
        "internal_url": internal_url,
        "model": os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b"),
        "timeout": float(os.getenv("OLLAMA_TIMEOUT", "180")),
        "num_predict": int(os.getenv("OLLAMA_NUM_PREDICT", "220")),
        "keep_alive": os.getenv("OLLAMA_KEEP_ALIVE", "30m"),
    }


@st.cache_data(ttl=15, show_spinner=False)
def get_ollama_status(internal_url: str, model: str):
    try:
        response = requests.get(f"{internal_url}/api/tags", timeout=5)
        response.raise_for_status()
        payload = response.json()
        models = [item.get("name", "") for item in payload.get("models", [])]
        return {
            "reachable": True,
            "ready": model in models,
            "available_models": models,
            "error": None,
        }
    except requests.RequestException as exc:
        return {
            "reachable": False,
            "ready": False,
            "available_models": [],
            "error": str(exc),
        }


def format_top_counts(series, top_n=5):
    counts = series.value_counts().head(top_n)
    if counts.empty:
        return "n/a"
    return ", ".join([f"{idx}: {val}" for idx, val in counts.items()])


def build_llm_context(filtered_df, date_range):
    total = len(filtered_df)
    unique_ies = filtered_df["ies"].nunique()
    program_count = filtered_df[
        ["carrera_norm", "campo_amplio", "nivel_formacion", "modalidad"]
    ].drop_duplicates().shape[0]
    provinces = filtered_df["provincia_norm"].nunique()
    cantons = filtered_df["canton_norm"].nunique()

    estado_summary = format_top_counts(filtered_df["estado_norm"], top_n=6)
    modalidad_summary = format_top_counts(filtered_df["modalidad"], top_n=6)
    top_ies = format_top_counts(filtered_df["ies"], top_n=10)
    top_prov = format_top_counts(filtered_df["provincia_norm"], top_n=10)
    top_canton = format_top_counts(filtered_df["canton_norm"], top_n=10)

    if date_range and len(date_range) == 2:
        date_text = f"{date_range[0]} to {date_range[1]}"
    else:
        date_text = "n/a"

    return textwrap.dedent(
        f"""
        Dataset filtered summary
        - Total active offers: {total}
        - Unique IES: {unique_ies}
        - Programs: {program_count}
        - Provinces covered: {provinces}
        - Cantons covered: {cantons}
        - Ingestion date range: {date_text}
        - Offer status distribution (top): {estado_summary}
        - Modality distribution (top): {modalidad_summary}
        - Top IES by offers: {top_ies}
        - Top provinces by offers: {top_prov}
        - Top cantons by offers: {top_canton}
        """
    ).strip()


def build_llm_prompt(context, question=None):
    base = textwrap.dedent(
        """
        Eres un analista de datos. Explica los resultados en espanol claro y simple
        para una persona que se complica con graficos y tablas.
        No inventes datos y solo usa la informacion provista.
        Entrega primero 5-8 bullets con hallazgos claros y luego un parrafo corto
        con posibles implicaciones o acciones.
        """
    ).strip()
    if question:
        base += f"\nPregunta del usuario: {question.strip()}"
    return f"{base}\n\nDatos:\n{context}"


def call_ollama(prompt):
    settings = get_ollama_settings()
    url = f"{settings['internal_url']}/api/generate"
    payload = {
        "model": settings["model"],
        "prompt": prompt,
        "stream": False,
        "keep_alive": settings["keep_alive"],
        "options": {
            "temperature": 0.2,
            "num_predict": settings["num_predict"],
        },
    }
    response = requests.post(url, json=payload, timeout=settings["timeout"])
    response.raise_for_status()
    data = response.json()
    return (data.get("response") or "").strip()


def explain_ollama_error(exc: Exception, settings: dict, status: dict) -> str:
    if isinstance(exc, requests.Timeout):
        return (
            "El modelo local tardo demasiado en responder. "
            f"Tiempo limite: {settings['timeout']:.0f}s. "
            "Prueba un modelo mas liviano como `qwen2.5:1.5b`, espera a que termine el precalentamiento, "
            "o aumenta `OLLAMA_TIMEOUT` si tu equipo usa CPU."
        )
    if not status.get("reachable"):
        return (
            "El dashboard no puede llegar a Ollama dentro de Docker. "
            f"Endpoint interno esperado: `{settings['internal_url']}`. "
            "Ese nombre solo existe dentro de la red Docker."
        )
    return f"LLM error: {exc}"


def safe_float(value, default=0.0):
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def score_label(score: float) -> str:
    if score >= 0.9:
        return "very strong"
    if score >= 0.8:
        return "strong"
    if score >= 0.7:
        return "useful"
    if score >= 0.6:
        return "moderate"
    return "weak"


def render_storage_links(paths_dict):
    if not paths_dict:
        st.info("No stored artifacts were found for this run.")
        return
    for artifact_name, info in paths_dict.items():
        if not isinstance(info, dict):
            st.markdown(f"- {artifact_name}: {info}")
            continue
        url = info.get("url")
        path = info.get("path", artifact_name)
        if url:
            st.markdown(f"- `{artifact_name}`: [{path}]({url})")
        else:
            st.markdown(f"- `{artifact_name}`: `{path}`")


def build_model_summary(
    latest_metrics,
    latest_oof_metrics,
    latest_cv_metrics,
    latest_run,
    candidate_rows,
    predictions,
    monitoring_metrics,
):
    f1_score_value = safe_float(latest_metrics.get("f1"))
    recall = safe_float(latest_metrics.get("recall"))
    precision = safe_float(latest_metrics.get("precision"))
    roc_auc = safe_float(latest_metrics.get("roc_auc"))
    candidate_count = len(candidate_rows)
    selected_candidate = latest_run.get("algorithm") or "n/a"
    storage_status = latest_run.get("storage_status") or "n/a"
    high_risk_count = 0
    if not predictions.empty and "risk_label" in predictions.columns:
        high_risk_count = int(predictions["risk_label"].fillna(False).astype(bool).sum())
    prediction_positive_rate = safe_float(monitoring_metrics.get("prediction_positive_rate"))

    summary = [
        (
            f"The active model is **{score_label(f1_score_value)}** overall: "
            f"F1 `{f1_score_value:.3f}` and ROC AUC `{roc_auc:.3f}`."
        ),
        (
            f"It is currently better at **finding risky rows** than being ultra conservative: "
            f"recall `{recall:.3f}` and precision `{precision:.3f}`."
        ),
        (
            f"The selected algorithm was **{selected_candidate}**, chosen after comparing "
            f"`{candidate_count}` candidate models with cross-validation."
        ),
        (
            "Training performance is estimated from true grouped out-of-fold "
            f"predictions: AP `{safe_float(latest_oof_metrics.get('average_precision')):.3f}` "
            f"and F1 `{safe_float(latest_oof_metrics.get('f1')):.3f}` at 0.5."
        ),
        (
            f"The production artifact currently flags about "
            f"`{prediction_positive_rate:.1%}` of rows as risky. These "
            "production scores are not used as evaluation evidence."
        ),
        (
            f"Stored artifacts are **{storage_status}**, and the current preview shows "
            f"`{high_risk_count}` high-risk rows in the latest scored sample."
        ),
    ]
    if latest_cv_metrics:
        summary.append(
            f"Cross-validation stayed stable: mean F1 `{safe_float(latest_cv_metrics.get('mean_f1')):.3f}` "
            f"with average precision `{safe_float(latest_cv_metrics.get('mean_average_precision')):.3f}`."
        )
    return summary


def build_monitoring_summary(monitoring_metrics):
    if not monitoring_metrics:
        return []
    prediction_positive_rate = safe_float(monitoring_metrics.get("prediction_positive_rate"))
    mean_probability = safe_float(monitoring_metrics.get("mean_risk_probability"))
    top10_probability = safe_float(monitoring_metrics.get("top_10_mean_probability"))
    return [
        f"On the full scored dataset, the model marked `{prediction_positive_rate:.1%}` of rows as risky.",
        f"The average predicted risk is `{mean_probability:.3f}`, which gives a sense of the model's general caution level.",
        f"The top 10 most suspicious rows have an average risk of `{top10_probability:.3f}`.",
        (
            "This is a feedback-capable, human-supervised snapshot; automated "
            f"drift detection is `{monitoring_metrics.get('drift_detection_executed', False)}` "
            f"and automatic retraining is `{monitoring_metrics.get('automatic_retraining', False)}`."
        ),
    ]


def build_ml_llm_context(
    latest_run,
    latest_metrics,
    latest_oof_metrics,
    latest_cv_metrics,
    candidate_rows,
    predictions,
    monitoring_metrics,
):
    top_predictions = predictions.head(10) if not predictions.empty else pd.DataFrame()
    top_risk_preview = "n/a"
    if not top_predictions.empty:
        preview_rows = []
        for _, row in top_predictions.iterrows():
            preview_rows.append(
                f"row {int(row['row_num'])} | risk={safe_float(row['risk_probability']):.3f} | "
                f"IES={row.get('nombre_ies', 'n/a')} | program={row.get('nombre_carrera', 'n/a')} | "
                f"province={row.get('provincia', 'n/a')} | state={row.get('estado', 'n/a')}"
            )
        top_risk_preview = "; ".join(preview_rows)

    candidate_text = "n/a"
    if candidate_rows:
        candidate_text = "; ".join(
            [
                (
                    f"{row.get('name')}: "
                    f"mean_f1={safe_float(row.get('mean_f1')):.3f}, "
                    f"mean_ap={safe_float(row.get('mean_average_precision')):.3f}"
                )
                for row in candidate_rows
            ]
        )

    return textwrap.dedent(
        f"""
        Predictive quality section summary
        - Model name: {latest_run.get('model_name')}
        - Model version: {latest_run.get('model_version')}
        - Selected candidate: {latest_run.get('algorithm')}
        - Primary scenario: {latest_run.get('primary_scenario')}
        - Storage status: {latest_run.get('storage_status')}
        - Holdout F1: {safe_float(latest_metrics.get('f1')):.3f}
        - Holdout precision: {safe_float(latest_metrics.get('precision')):.3f}
        - Holdout recall: {safe_float(latest_metrics.get('recall')):.3f}
        - Holdout ROC AUC: {safe_float(latest_metrics.get('roc_auc')):.3f}
        - Holdout average precision: {safe_float(latest_metrics.get('average_precision')):.3f}
        - Grouped OOF F1 at 0.5: {safe_float(latest_oof_metrics.get('f1')):.3f}
        - Grouped OOF average precision: {safe_float(latest_oof_metrics.get('average_precision')):.3f}
        - CV mean F1: {safe_float(latest_cv_metrics.get('mean_f1')):.3f}
        - CV mean average precision: {safe_float(latest_cv_metrics.get('mean_average_precision')):.3f}
        - Candidate comparison: {candidate_text}
        - Monitoring prediction positive rate: {safe_float(monitoring_metrics.get('prediction_positive_rate')):.3%}
        - Monitoring mean risk probability: {safe_float(monitoring_metrics.get('mean_risk_probability')):.3f}
        - Drift detection executed: {monitoring_metrics.get('drift_detection_executed', False)}
        - Automatic retraining: {monitoring_metrics.get('automatic_retraining', False)}
        - Highest risk sample rows: {top_risk_preview}
        """
    ).strip()


def build_ml_focus_prompt(section_focus: str) -> str:
    prompts = {
        "Executive summary": (
            "Summarize the predictive section for a non-technical research reader. "
            "Explain whether the model is trustworthy, what it is good at, and what decisions should be made first."
        ),
        "Candidate models": (
            "Compare the candidate models in simple language. "
            "Explain why the winning model likely won, what trade-offs exist, and whether the selection looks robust."
        ),
        "High-risk rows": (
            "Explain what the high-risk rows mean in practice. "
            "Describe what patterns deserve manual review first and what a research team should do next."
        ),
        "Monitoring snapshot": (
            "Interpret the monitoring metrics in simple language. "
            "Explain whether the live scoring behaviour looks stable and what warning signs should be watched."
        ),
    }
    return prompts.get(section_focus, prompts["Executive summary"])


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

tab_overview, tab_geo, tab_diversity, tab_quality, tab_timeline, tab_monitoring, tab_mlops = st.tabs(
    ["Overview", "Geographic Coverage", "Diversity & Institutions", "Data Quality", "Timeline", "Monitoring", "AI & Risk Analysis"]
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
        show_dataframe(top_ies)

        st.subheader("Interpretacion con LLM")
        settings = get_ollama_settings()
        ollama_status = get_ollama_status(settings["internal_url"], settings["model"])
        st.caption(f"Modelo local: {settings['model']}")
        st.caption(
            "La IA local corre dentro de Docker y el dashboard se conecta internamente a Ollama."
        )
        if ollama_status["ready"]:
            st.success("Ollama esta listo y el modelo ya fue cargado en el contenedor.")
        elif ollama_status["reachable"]:
            st.warning(
                "Ollama responde, pero el modelo todavia no aparece listo. "
                "Espera un poco mas a que termine la descarga o el precalentamiento."
            )
        else:
            st.warning(
                "Ollama no esta disponible todavia desde el dashboard. "
                "Revisa `docker compose logs -f ollama`."
            )
        llm_question = st.text_input(
            "Pregunta opcional para el LLM",
            value="",
            key="llm_question"
        )
        if st.button("Interpretar resultados", type="primary", key="llm_interpret"):
            with st.spinner("Generando interpretacion..."):
                try:
                    if not ollama_status["ready"]:
                        raise RuntimeError(
                            "El modelo no esta listo aun. Espera a que termine la preparacion de Ollama."
                        )
                    context = build_llm_context(filtered, date_range)
                    prompt = build_llm_prompt(context, llm_question)
                    response = call_ollama(prompt)
                    if response:
                        st.session_state["llm_response"] = response
                    else:
                        st.session_state["llm_response"] = "No se recibio respuesta del modelo."
                except requests.RequestException as exc:
                    st.error(explain_ollama_error(exc, settings, ollama_status))
                except RuntimeError as exc:
                    st.error(str(exc))
                except Exception as exc:
                    st.error(f"LLM error inesperado: {exc}")

        if st.session_state.get("llm_response"):
            st.markdown(st.session_state["llm_response"])
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
            show_dataframe(prov_counts.sort_values("offers", ascending=False).head(10))
        with col2:
            st.subheader("Top Cantons by Offer Volume")
            canton_top = filtered.groupby("canton_norm").size().reset_index(name="offers").sort_values("offers", ascending=False)
            show_dataframe(canton_top.head(10))

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
        show_dataframe(prov_diversity.sort_values("hhi", ascending=False).head(10))

        st.subheader("Most Diversified Provinces (Low HHI)")
        show_dataframe(prov_diversity.sort_values("hhi", ascending=True).head(10))

        canton_hhi = filtered.groupby("canton_norm")["campo_amplio"].apply(hhi).reset_index(name="hhi")
        canton_entropy = filtered.groupby("canton_norm")["campo_amplio"].apply(entropy).reset_index(name="entropy")
        canton_diversity = canton_hhi.merge(canton_entropy, on="canton_norm", how="left")

        st.subheader("Most Specialized Cantons (High HHI)")
        show_dataframe(canton_diversity.sort_values("hhi", ascending=False).head(10))

        st.subheader("Most Diversified Cantons (Low HHI)")
        show_dataframe(canton_diversity.sort_values("hhi", ascending=True).head(10))

        st.subheader("Institution Profiling")
        ies_counts = filtered.groupby("ies").size().reset_index(name="offers").sort_values("offers", ascending=False)
        ies_fields = filtered.groupby("ies")["campo_amplio"].nunique().reset_index(name="unique_fields")
        ies_levels = filtered.groupby("ies")["nivel_formacion"].nunique().reset_index(name="unique_levels")
        ies_profile = ies_counts.merge(ies_fields, on="ies", how="left").merge(ies_levels, on="ies", how="left")
        show_dataframe(ies_profile.head(15))

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
        show_dataframe(filtered_issues)

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
    show_dataframe(files)

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
            show_dataframe(latest_steps[["step_name", "duration_seconds", "row_count", "started_at", "detail"]])
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
        show_dataframe(latest[["service_name", "status", "status_code", "latency_ms", "created_at", "endpoint"]])

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
            show_dataframe(artifact_df, use_container_width=True)
        else:
            st.info("Storage uploads are not available yet. Check SUPABASE_SERVICE_ROLE_KEY and rerun ETL.")

with tab_mlops:
    st.header("AI & Risk Analysis")
    st.caption(
        "This section explains the predictive model in plain language: how reliable it is, "
        "what kind of risky rows it catches, and where to inspect first."
    )

    try:
        training_runs = load_data("""
            SELECT
                run_id,
                file_id,
                model_name,
                model_version,
                task_name,
                algorithm,
                target_name,
                status,
                started_at,
                finished_at,
                duration_seconds,
                train_rows,
                test_rows,
                positive_rows,
                positive_rate,
                selected_metric,
                oof_metrics,
                cv_metrics,
                candidate_metrics,
                metrics,
                operational_metrics,
                operational_threshold,
                threshold_policy,
                primary_scenario,
                artifact_path,
                storage_status,
                storage_paths,
                is_active
            FROM mlops.training_runs
            ORDER BY started_at DESC
        """)
    except Exception:
        training_runs = pd.DataFrame()

    if training_runs.empty:
        st.info("No ML training runs found yet. The `ml-trainer` service populates this section.")
    else:
        training_runs["metrics_dict"] = training_runs["metrics"].apply(parse_metrics)
        training_runs["oof_metrics_dict"] = training_runs["oof_metrics"].apply(parse_metrics)
        training_runs["cv_metrics_dict"] = training_runs["cv_metrics"].apply(parse_metrics)
        training_runs["candidate_metrics_dict"] = training_runs["candidate_metrics"].apply(parse_metrics)
        training_runs["operational_metrics_dict"] = training_runs["operational_metrics"].apply(parse_metrics)
        training_runs["storage_paths_dict"] = training_runs["storage_paths"].apply(parse_json_value)

        latest_run = training_runs.iloc[0]
        latest_metrics = latest_run["metrics_dict"] or {}
        latest_oof_metrics = latest_run["oof_metrics_dict"] or {}
        latest_cv_metrics = latest_run["cv_metrics_dict"] or {}
        latest_operational_metrics = latest_run["operational_metrics_dict"] or {}
        latest_candidates = latest_run["candidate_metrics_dict"] or {}
        candidate_rows = latest_candidates.get("candidates") or []

        try:
            predictions = load_data("""
                SELECT
                    row_num,
                    natural_key,
                    risk_probability,
                    risk_label,
                    actual_label,
                    nombre_ies,
                    nombre_carrera,
                    provincia,
                    canton,
                    estado,
                    model_version,
                    prediction_origin,
                    scenario,
                    fold_id
                FROM mlops.v_latest_quality_risk_predictions
                WHERE prediction_origin = 'production_inference'
                ORDER BY risk_probability DESC, row_num ASC
                LIMIT 200
            """)
        except Exception:
            predictions = pd.DataFrame()

        try:
            monitoring_runs = load_data("""
                SELECT monitor_id, run_id, file_id, created_at, metrics
                FROM mlops.monitoring_runs
                ORDER BY created_at DESC
            """)
        except Exception:
            monitoring_runs = pd.DataFrame()

        latest_monitoring_metrics = {}
        if not monitoring_runs.empty:
            monitoring_runs["metrics_dict"] = monitoring_runs["metrics"].apply(parse_metrics)
            latest_monitoring_metrics = monitoring_runs.iloc[0]["metrics_dict"] or {}

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Current Model", latest_run.get("model_name") or "n/a")
        m2.metric("Version", latest_run.get("model_version") or "n/a")
        m3.metric("Balanced Score (F1)", f"{safe_float(latest_metrics.get('f1')):.3f}")
        m4.metric("Ranking Quality (ROC AUC)", f"{safe_float(latest_metrics.get('roc_auc')):.3f}")

        g1, g2, g3, g4 = st.columns(4)
        g1.metric("Winning Algorithm", latest_run.get("algorithm") or "n/a")
        g2.metric("Selection Metric", latest_run.get("selected_metric") or "n/a")
        g3.metric(
            "Operational Threshold",
            f"{safe_float(latest_run.get('operational_threshold'), 0.5):.2f}",
        )
        g4.metric("Stored Files", latest_run.get("storage_status") or "n/a")

        st.subheader("What This Means")
        for summary_line in build_model_summary(
            latest_metrics,
            latest_oof_metrics,
            latest_cv_metrics,
            latest_run,
            candidate_rows,
            predictions,
            latest_monitoring_metrics,
        ):
            st.markdown(f"- {summary_line}")

        help_col1, help_col2 = st.columns([2, 1])
        with help_col1:
            st.info(
                "Quick guide: `precision` tells you how often a risky alert is correct, "
                "`recall` tells you how many problematic rows the model manages to catch, "
                "and `F1` balances both."
            )
        with help_col2:
            if latest_run.get("storage_status") == "success":
                st.success("The active model is persisted.")
            else:
                st.warning("The active model is not fully persisted yet.")

        st.subheader("Reliability Breakdown")
        reliability_cols = st.columns(4)
        reliability_metrics = [
            ("Precision", safe_float(latest_metrics.get("precision"))),
            ("Recall", safe_float(latest_metrics.get("recall"))),
            ("F1", safe_float(latest_metrics.get("f1"))),
            ("Average Precision", safe_float(latest_metrics.get("average_precision"))),
        ]
        for column, (label, value) in zip(reliability_cols, reliability_metrics):
            with column:
                st.metric(label, f"{value:.3f}")
                st.progress(max(0, min(int(round(value * 100)), 100)), text=f"{value:.0%}")

        st.subheader("AI Explanation")
        settings = get_ollama_settings()
        ollama_status = get_ollama_status(settings["internal_url"], settings["model"])
        ml_focus = st.selectbox(
            "What should the AI explain?",
            [
                "Executive summary",
                "Candidate models",
                "High-risk rows",
                "Monitoring snapshot",
            ],
            index=0,
            key="ml_llm_focus",
        )
        ml_question = st.text_input(
            "Optional question about this section",
            value="Explain in simple words if the model is reliable and what should be reviewed first.",
            key="ml_llm_question",
        )
        if st.button("Explain this predictive section with local AI", key="ml_llm_button"):
            with st.spinner("Generating explanation..."):
                try:
                    if not ollama_status["ready"]:
                        raise RuntimeError(
                            "The local model is not ready yet. Wait for Ollama warmup to finish."
                        )
                    ml_context = build_ml_llm_context(
                        latest_run,
                        latest_metrics,
                        latest_oof_metrics,
                        latest_cv_metrics,
                        candidate_rows,
                        predictions,
                        latest_monitoring_metrics,
                    )
                    focused_prompt = (
                        f"{build_ml_focus_prompt(ml_focus)} "
                        f"Additional user question: {ml_question.strip() or 'none'}"
                    )
                    st.session_state["ml_llm_response"] = call_ollama(
                        build_llm_prompt(ml_context, focused_prompt)
                    )
                except requests.RequestException as exc:
                    st.error(explain_ollama_error(exc, settings, ollama_status))
                except RuntimeError as exc:
                    st.error(str(exc))
                except Exception as exc:
                    st.error(f"LLM error inesperado: {exc}")

        if st.session_state.get("ml_llm_response"):
            st.markdown(st.session_state["ml_llm_response"])

        if candidate_rows:
            st.subheader("Candidate Model Comparison")
            candidate_df = pd.DataFrame(candidate_rows)
            candidate_df = candidate_df.sort_values(
                by=["mean_average_precision", "mean_f1"],
                ascending=False,
            )
            winner_name = latest_run.get("algorithm")
            if winner_name:
                st.success(f"Selected winner: `{winner_name}`")
            fig_candidates = px.bar(
                candidate_df,
                x="name",
                y=["mean_average_precision", "mean_f1"],
                barmode="group",
                title="Candidate Model Comparison"
            )
            st.plotly_chart(fig_candidates, use_container_width=True)
            with st.expander("Open detailed candidate metrics"):
                show_dataframe(candidate_df, use_container_width=True)

        history_rows = []
        for _, row in training_runs.iterrows():
            metrics = row.get("metrics_dict") or {}
            history_rows.append({
                "started_at": row.get("started_at"),
                "model_version": row.get("model_version"),
                "f1": metrics.get("f1"),
                "precision": metrics.get("precision"),
                "recall": metrics.get("recall"),
                "roc_auc": metrics.get("roc_auc"),
            })
        history_df = pd.DataFrame(history_rows).dropna(subset=["started_at"])
        if not history_df.empty:
            history_long = history_df.melt(
                id_vars=["started_at", "model_version"],
                var_name="metric",
                value_name="value"
            ).dropna(subset=["value"])
            if not history_long.empty:
                fig_history = px.line(
                    history_long,
                    x="started_at",
                    y="value",
                    color="metric",
                    markers=True,
                    hover_data=["model_version"],
                    title="Model Metrics Over Time"
                )
                st.plotly_chart(fig_history, use_container_width=True)

        try:
            importances = load_data("""
                SELECT run_id, feature_name, importance, direction, rank
                FROM mlops.feature_importance
                WHERE run_id = :run_id
                ORDER BY rank ASC
            """, {"run_id": latest_run["run_id"]})
        except Exception:
            importances = pd.DataFrame()

        if not importances.empty:
            st.subheader("What Drives the Risk Score")
            fig_importance = px.bar(
                importances.sort_values("rank"),
                x="importance",
                y="feature_name",
                color="direction",
                orientation="h",
                title="Top Drivers of Quality Risk"
            )
            st.plotly_chart(fig_importance, use_container_width=True)
            with st.expander("Open feature importance details"):
                show_dataframe(importances, use_container_width=True)

        if not predictions.empty:
            st.subheader("Highest Risk Records")
            preview = predictions.copy()
            preview["risk_percent"] = preview["risk_probability"].apply(lambda value: round(safe_float(value) * 100, 1))
            risk_threshold = safe_float(
                latest_run.get("operational_threshold"),
                0.5,
            )
            p1, p2, p3 = st.columns(3)
            p1.metric("Rows in Preview", len(preview))
            p2.metric("Rows Above Threshold", int(preview["risk_label"].fillna(False).astype(bool).sum()))
            p3.metric("Risk Threshold", f"{risk_threshold:.2f}")

            fig_risk = px.histogram(
                preview,
                x="risk_probability",
                nbins=20,
                color="actual_label",
                title="Risk Probability Distribution"
            )
            st.plotly_chart(fig_risk, use_container_width=True)
            st.caption("Preview below is limited to the highest-risk rows so the review starts with the most suspicious cases.")
            show_dataframe(
                preview.head(25)[
                    [
                        "row_num",
                        "risk_percent",
                        "risk_label",
                        "actual_label",
                        "nombre_ies",
                        "nombre_carrera",
                        "provincia",
                        "canton",
                        "estado",
                    ]
                ],
                use_container_width=True,
            )

        if latest_monitoring_metrics:
            st.subheader("Monitoring Snapshot")
            for summary_line in build_monitoring_summary(latest_monitoring_metrics):
                st.markdown(f"- {summary_line}")
            with st.expander("Open latest monitoring details"):
                st.json(latest_monitoring_metrics)

        with st.expander("Open stored model files"):
            render_storage_links(latest_run.get("storage_paths_dict"))

        with st.expander("Open latest training run details"):
            st.json({
                "run_id": latest_run.get("run_id"),
                "model_name": latest_run.get("model_name"),
                "model_version": latest_run.get("model_version"),
                "algorithm": latest_run.get("algorithm"),
                "target_name": latest_run.get("target_name"),
                "status": latest_run.get("status"),
                "artifact_path": latest_run.get("artifact_path"),
                "storage_status": latest_run.get("storage_status"),
                "storage_paths": latest_run.get("storage_paths_dict"),
                "metrics": latest_metrics,
                "oof_metrics": latest_oof_metrics,
                "cv_metrics": latest_cv_metrics,
                "operational_metrics": latest_operational_metrics,
                "operational_threshold": latest_run.get(
                    "operational_threshold"
                ),
                "threshold_policy": latest_run.get("threshold_policy"),
                "primary_scenario": latest_run.get("primary_scenario"),
            })

        with st.expander("Open training run history"):
            show_dataframe(
                training_runs.drop(
                    columns=[
                        "metrics_dict",
                        "oof_metrics_dict",
                        "cv_metrics_dict",
                        "candidate_metrics_dict",
                        "operational_metrics_dict",
                        "storage_paths_dict",
                    ]
                ),
                use_container_width=True,
            )
