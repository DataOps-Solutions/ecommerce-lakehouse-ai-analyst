"""Demo interactivo para dueños y gerentes de e-commerce.

Ejecución local::

    make ui        # o: streamlit run app/dashboard.py

Las cifras provienen de los mismos módulos de ``src/`` que usan el CLI y los reportes.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from app.services import (
    SOURCE_SAMPLE,
    SOURCE_SYNTHETIC,
    SOURCE_UPLOAD,
    SUGGESTED_QUESTIONS,
    UPLOAD_LABELS,
    UPLOAD_SOURCES,
    Dataset,
    UploadError,
    ask,
    category_frame,
    channel_frame,
    daily_frame,
    fmt_int,
    fmt_money,
    fmt_pct,
    kpis,
    load_sample,
    load_synthetic,
    load_uploaded,
    privacy_frame,
    quality_frame,
    quarantine_frame,
    stock_alert_frame,
    top_return_skus,
)
from src.config import get_settings

PLOT_COLORS = ["#1F6F8B", "#2BA84A", "#F2A541", "#C8553D", "#6C5B7B"]


@st.cache_data(show_spinner="Procesando la demo de muestra...")
def _cached_sample() -> Dataset:
    return load_sample(get_settings())


@st.cache_data(show_spinner="Generando el dataset multicanal...")
def _cached_synthetic() -> Dataset:
    return load_synthetic(get_settings())


def _sources_available() -> list[str]:
    options = [SOURCE_SAMPLE, SOURCE_SYNTHETIC]
    if st.session_state.get("uploaded") is not None:
        options.append(SOURCE_UPLOAD)
    return options


def _load_source(name: str) -> Dataset:
    if name == SOURCE_SYNTHETIC:
        return _cached_synthetic()
    if name == SOURCE_UPLOAD:
        dataset = st.session_state.get("uploaded")
        if isinstance(dataset, Dataset):
            return dataset
    return _cached_sample()


def _style_figure(fig: go.Figure) -> None:
    fig.update_layout(
        margin={"l": 10, "r": 10, "t": 40, "b": 10},
        legend_title_text="",
        plot_bgcolor="white",
        paper_bgcolor="white",
        font={"family": "Inter, Segoe UI, sans-serif", "size": 13},
    )


def _kpi_row(ds: Dataset) -> None:
    k = kpis(ds)
    cols = st.columns(4)
    cols[0].metric("Ventas brutas", fmt_money(k.gross_sales))
    cols[1].metric("Ingreso neto", fmt_money(k.net_revenue))
    cols[2].metric("Margen neto real", fmt_money(k.net_margin), fmt_pct(k.margin_pct))
    cols[3].metric("Unidades vendidas", fmt_int(k.units), f"{fmt_int(k.orders)} pedidos")


def _render_summary(ds: Dataset) -> None:
    st.subheader(f"Resumen ejecutivo · {ds.name}")
    if ds.totals is None:
        st.warning("No hay ventas válidas en esta fuente para mostrar cifras.")
        return
    _kpi_row(ds)

    st.markdown("#### Canales")
    channels = channel_frame(ds)
    left, right = st.columns(2)
    fig_channel = px.bar(
        channels,
        x="Canal",
        y=["Ingreso neto", "Margen neto"],
        barmode="group",
        color_discrete_sequence=PLOT_COLORS,
        title="Ingreso neto y margen neto por canal",
    )
    _style_figure(fig_channel)
    left.plotly_chart(fig_channel, use_container_width=True)
    table = channels.copy()
    for column in ("Ingreso neto", "Margen neto"):
        table[column] = table[column].map(fmt_money)
    table["Margen %"] = table["Margen %"].map(fmt_pct)
    right.dataframe(table, hide_index=True, use_container_width=True)

    st.markdown("#### Devoluciones y reposición")
    cat = category_frame(ds)
    c1, c2 = st.columns(2)
    fig_returns = px.bar(
        cat.sort_values("Tasa de devolución", ascending=False),
        x="Categoría",
        y="Tasa de devolución",
        color_discrete_sequence=[PLOT_COLORS[3]],
        title="Tasa de devolución por categoría",
    )
    fig_returns.update_yaxes(tickformat=".0%")
    _style_figure(fig_returns)
    c1.plotly_chart(fig_returns, use_container_width=True)
    fig_alerts = px.bar(
        cat,
        x="Categoría",
        y="SKU en alerta de stock",
        color_discrete_sequence=[PLOT_COLORS[2]],
        title="SKU con stock agotado o por reponer",
    )
    _style_figure(fig_alerts)
    c2.plotly_chart(fig_alerts, use_container_width=True)

    top = top_return_skus(ds)
    if not top.empty:
        fig_top = px.bar(
            top.sort_values("Tasa de devolución"),
            x="Tasa de devolución",
            y="SKU",
            orientation="h",
            hover_data=["Producto", "Vendidas", "Devueltas"],
            color_discrete_sequence=[PLOT_COLORS[3]],
            title="Top SKUs por tasa de devolución (mínimo 5 unidades vendidas)",
        )
        fig_top.update_xaxes(tickformat=".0%")
        _style_figure(fig_top)
        st.plotly_chart(fig_top, use_container_width=True)
    st.caption("Estado de stock por SKU")
    st.dataframe(stock_alert_frame(ds), hide_index=True, use_container_width=True)

    st.markdown("#### Evolución diaria")
    daily = daily_frame(ds)
    fig_daily = px.line(
        daily,
        x="Fecha",
        y=["Ventas brutas", "Ingreso neto"],
        color_discrete_sequence=PLOT_COLORS,
        title="Ventas e ingreso neto por día",
    )
    _style_figure(fig_daily)
    st.plotly_chart(fig_daily, use_container_width=True)

    _render_transparency(ds)


def _render_transparency(ds: Dataset) -> None:
    st.markdown("#### Transparencia y calidad del dato")
    summary = ds.summary
    exact_cost = summary.cost_exact_share
    exact_fee = summary.fee_exact_share
    cols = st.columns(3)
    cols[0].metric(
        "Líneas con costo exacto",
        fmt_pct(exact_cost, 1),
        help=(
            "Costo unitario informado en la planilla. "
            "El resto usa un supuesto marcado como estimado."
        ),
    )
    cols[1].metric(
        "Pagos con comisión exacta",
        fmt_pct(exact_fee, 1),
        help="Comisión informada por la pasarela. El resto se estima con la tasa configurada.",
    )
    cols[2].metric(
        "Líneas de venta excluidas",
        fmt_int(summary.excluded_sales_lines),
        summary.excluded_sales_amount,
        delta_color="off",
    )
    if summary.caveats:
        st.markdown("**Advertencias operativas**")
        for caveat in summary.caveats:
            st.markdown(f"- {caveat}")
    quality = quality_frame(ds)
    if not quality.empty:
        st.dataframe(quality, hide_index=True, use_container_width=True)


def _render_analyst(ds: Dataset) -> None:
    st.subheader("Preguntas de negocio")
    st.caption(
        "Escriba una pregunta en lenguaje natural o use una sugerencia. Las respuestas salen "
        "solo de las tablas autorizadas y se validan antes de ejecutarse."
    )
    buttons = st.columns(len(SUGGESTED_QUESTIONS))
    for index, question in enumerate(SUGGESTED_QUESTIONS):
        if buttons[index].button(question, key=f"suggest_{index}", use_container_width=True):
            st.session_state["question"] = question
    typed = st.text_input(
        "Su pregunta",
        key="question",
        placeholder="Por ejemplo: ¿Cuál fue el margen neto por canal?",
    )
    if not typed:
        return
    response = ask(get_settings(), ds, typed)
    if response.status != "ok":
        st.info(response.error or "No puedo responder esa pregunta con los datos disponibles.")
        return
    st.markdown("#### Respuesta")
    if response.interpretation:
        st.success(response.interpretation)
    frame = pd.DataFrame(response.rows, columns=response.columns)
    st.dataframe(frame, hide_index=True, use_container_width=True)
    with st.expander("Ver consulta SQL auditada"):
        st.caption(f"Tablas consultadas: {', '.join(response.tables)}")
        st.code(response.sql or "", language="sql")


def _render_uploads() -> None:
    st.subheader("Cargue sus archivos y vea el resultado al instante")
    st.caption(
        "Suba las exportaciones de su tienda. Los nombres de columna se reconocen según los "
        "mapeos de `mappings/`. Los datos personales no se guardan en claro."
    )
    cols = st.columns(len(UPLOAD_SOURCES))
    files: dict[str, bytes] = {}
    for column, source in zip(cols, UPLOAD_SOURCES, strict=True):
        uploaded = column.file_uploader(UPLOAD_LABELS[source], type=["csv"], key=f"up_{source}")
        if uploaded is not None:
            files[source] = uploaded.getvalue()
    if st.button("Procesar archivos", type="primary", disabled=not files):
        try:
            with st.spinner("Procesando sus archivos..."):
                st.session_state["uploaded"] = load_uploaded(get_settings(), files)
            st.success("Archivos procesados. Ya puede verlos en la barra lateral.")
        except UploadError as exc:
            st.error(str(exc))
    dataset = st.session_state.get("uploaded")
    if not isinstance(dataset, Dataset):
        return
    _kpi_row(dataset)
    left, right = st.columns([1, 1])
    left.markdown("#### Protección de datos personales")
    privacy = privacy_frame(dataset)
    if privacy.empty:
        left.info("No se encontraron campos personales en los archivos.")
    else:
        left.dataframe(privacy, hide_index=True, use_container_width=True)
    right.markdown("#### Calidad de la carga")
    right.dataframe(quality_frame(dataset), hide_index=True, use_container_width=True)

    st.markdown("#### Filas en cuarentena")
    st.caption(
        "Filas que no pasaron los controles de calidad. Puede corregirlas en su sistema y "
        "volver a cargar el archivo."
    )
    quarantine = quarantine_frame(dataset)
    if quarantine.empty:
        st.success("No hay filas en cuarentena.")
        return
    st.dataframe(quarantine, hide_index=True, use_container_width=True)
    st.download_button(
        "Descargar filas en cuarentena (CSV)",
        data=quarantine.to_csv(index=False, sep=";").encode("utf-8-sig"),
        file_name="filas_en_cuarentena.csv",
        mime="text/csv",
    )


def main() -> None:
    st.set_page_config(
        page_title="Análisis de ventas y márgenes",
        page_icon="📊",
        layout="wide",
    )
    st.title("Análisis de ventas y márgenes")
    st.caption("Ventas, márgenes reales, devoluciones y stock, con transparencia sobre el dato.")

    with st.sidebar:
        st.header("Fuente de datos")
        source = st.radio("Elija la fuente", _sources_available(), key="source")
        st.divider()
        st.markdown(
            "Para ver el flujo completo de carga y privacidad, use la pestaña "
            "**Carga de archivos y privacidad**."
        )

    tab_summary, tab_analyst, tab_upload = st.tabs(
        ["Resumen ejecutivo", "Analista de negocio", "Carga de archivos y privacidad"]
    )
    with tab_summary:
        _render_summary(_load_source(source))
    with tab_analyst:
        _render_analyst(_load_source(source))
    with tab_upload:
        _render_uploads()


main()
