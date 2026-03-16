# from pathlib import Path
# import sys
# from datetime import datetime

# import os
# API_URL = os.getenv("API_URL", "http://127.0.0.1:8000/predict_batch")

# PROJECT_ROOT = Path(__file__).resolve().parents[2]
# if str(PROJECT_ROOT) not in sys.path:
#     sys.path.insert(0, str(PROJECT_ROOT))

# import pandas as pd
# import streamlit as st
# import plotly.express as px
# import plotly.graph_objects as go
# # import subprocess

# from src.analytics.kpis import compute_kpis
# from src.analytics.reporting import (
#     sla_by_operator,
#     sla_by_month,
#     breach_by_service,
#     breach_by_receiver_bank,
#     loop_count_summary,
#     daily_summary,
# )
# from src.analytics.forecasting import daily_transaction_counts, forecast_transaction_volume
# from src.analytics.insights import generate_insights


# # -------------------------
# # Page config
# # -------------------------
# st.set_page_config(
#     page_title="Payment Operations SLA Intelligence Dashboard",
#     layout="wide",
#     initial_sidebar_state="expanded",
# )

# # -------------------------
# # Custom CSS
# # -------------------------
# st.markdown("""
# <style>
#     .main {
#         background-color: #f5f7fb;
#     }

#     .block-container {
#         padding-top: 1.2rem;
#         padding-bottom: 2rem;
#         padding-left: 2rem;
#         padding-right: 2rem;
#         max-width: 1800px;
#     }

#     .hero {
#         background: linear-gradient(135deg, #1f3c88 0%, #2f6fed 100%);
#         padding: 1.5rem 1.8rem;
#         border-radius: 20px;
#         color: white;
#         margin-bottom: 1.2rem;
#         box-shadow: 0 8px 24px rgba(31, 60, 136, 0.18);
#     }

#     .hero-title {
#         font-size: 2.2rem;
#         font-weight: 800;
#         margin-bottom: 0.2rem;
#     }

#     .hero-subtitle {
#         font-size: 1rem;
#         opacity: 0.95;
#         margin-bottom: 0.6rem;
#     }

#     .hero-meta {
#         font-size: 0.9rem;
#         opacity: 0.9;
#     }

#     .section-title {
#         font-size: 1.25rem;
#         font-weight: 700;
#         color: #1f2a44;
#         margin-top: 0.2rem;
#         margin-bottom: 0.7rem;
#     }

#     .kpi-card {
#         background: white;
#         padding: 1rem 1.1rem;
#         border-radius: 16px;
#         box-shadow: 0 4px 14px rgba(0,0,0,0.05);
#         margin-bottom: 0.9rem;
#         border: 1px solid #edf1f7;
#     }

#     .kpi-blue { border-left: 6px solid #2E86DE; }
#     .kpi-green { border-left: 6px solid #00A878; }
#     .kpi-red { border-left: 6px solid #E45756; }
#     .kpi-orange { border-left: 6px solid #F39C12; }
#     .kpi-purple { border-left: 6px solid #7D3C98; }

#     .kpi-title {
#         font-size: 0.92rem;
#         color: #6b7280;
#         margin-bottom: 0.35rem;
#     }

#     .kpi-value {
#         font-size: 2rem;
#         font-weight: 800;
#         color: #111827;
#         line-height: 1.1;
#     }

#     .kpi-note {
#         font-size: 0.78rem;
#         color: #7b8190;
#         margin-top: 0.35rem;
#     }

#     .insight-box {
#         background: #ffffff;
#         border-left: 5px solid #6C5CE7;
#         border-radius: 12px;
#         padding: 0.9rem 1rem;
#         box-shadow: 0 3px 10px rgba(0,0,0,0.04);
#         margin-bottom: 0.65rem;
#         color: #1f2937;
#     }

#     .pill {
#         display: inline-block;
#         padding: 0.25rem 0.7rem;
#         border-radius: 999px;
#         font-size: 0.8rem;
#         font-weight: 600;
#         margin-right: 0.35rem;
#         margin-top: 0.2rem;
#     }

#     .pill-blue { background: #e8f1ff; color: #1d4ed8; }
#     .pill-red { background: #ffeaea; color: #c0392b; }
#     .pill-green { background: #eafaf3; color: #0f8f5b; }

#     div[data-testid="stDataFrame"] {
#         background: white;
#         border-radius: 14px;
#         padding: 0.2rem;
#         border: 1px solid #eef2f7;
#     }
# </style>
# """, unsafe_allow_html=True)

# # -------------------------
# # Paths
# # -------------------------
# SCORED_PATH = Path("data/processed/scored_transactions.csv")

# # -------------------------
# # Helpers
# # -------------------------
# @st.cache_data
# def load_scored_data(path: str) -> pd.DataFrame:
#     return pd.read_csv(path, low_memory=False)


# # def run_scoring_script():
# #     result = subprocess.run(
# #         [sys.executable, "-m", "src.serving.score_dataset"],
# #         capture_output=True,
# #         text=True
# #     )
# #     return result


# def to_csv_bytes(df: pd.DataFrame) -> bytes:
#     return df.to_csv(index=False).encode("utf-8")


# def render_kpi_card(title, value, note="", tone="blue"):
#     tone_class = {
#         "blue": "kpi-blue",
#         "green": "kpi-green",
#         "red": "kpi-red",
#         "orange": "kpi-orange",
#         "purple": "kpi-purple",
#     }.get(tone, "kpi-blue")

#     st.markdown(
#         f"""
#         <div class="kpi-card {tone_class}">
#             <div class="kpi-title">{title}</div>
#             <div class="kpi-value">{value}</div>
#             <div class="kpi-note">{note}</div>
#         </div>
#         """,
#         unsafe_allow_html=True
#     )


# def style_plotly(fig):
#     fig.update_layout(
#         template="plotly_white",
#         paper_bgcolor="white",
#         plot_bgcolor="white",
#         font=dict(family="Arial, sans-serif", size=13, color="#1f2937"),
#         title=dict(font=dict(size=22, color="#1f2a44")),
#         margin=dict(l=20, r=20, t=60, b=20),
#         legend=dict(
#             orientation="h",
#             yanchor="bottom",
#             y=1.02,
#             xanchor="right",
#             x=1
#         ),
#         hovermode="x unified"
#     )
#     fig.update_xaxes(showgrid=False)
#     fig.update_yaxes(gridcolor="#E5E7EB")
#     return fig


# def safe_pct(x):
#     return f"{x:.2%}" if x is not None and pd.notna(x) else "N/A"


# def safe_num(x, decimals=2):
#     return f"{x:.{decimals}f}" if x is not None and pd.notna(x) else "N/A"


# # -------------------------
# # Sidebar
# # -------------------------
# # st.sidebar.header("Controls")

# # if st.sidebar.button("Refresh scored dataset"):
# #     with st.spinner("Running scoring pipeline..."):
# #         result = run_scoring_script()
# #     if result.returncode == 0:
# #         st.sidebar.success("Scored dataset refreshed.")
# #         st.cache_data.clear()
# #     else:
# #         st.sidebar.error("Scoring failed.")
# #         st.sidebar.text(result.stderr)

# st.sidebar.header("Controls")
# st.sidebar.info("This cloud deployment is a dashboard-only demo built from a pre-scored dataset.")

# # if not SCORED_PATH.exists():
# #     st.error("scored_transactions.csv not found. Run the scoring step first.")
# #     st.stop()

# if not SCORED_PATH.exists():
#     st.error("scored_transactions.csv not found in the app repository. Add the demo scored dataset to data/processed/ and redeploy.")
#     st.stop()

# st.sidebar.header("Controls")
# st.sidebar.info("This cloud deployment is a dashboard-only demo built from a pre-scored dataset.")

# df = load_scored_data(str(SCORED_PATH))

# if "start_time" in df.columns:
#     df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")

# for col in ["Message_Type", "Sender_Bank", "Receiver_Bank", "Service_Name", "Currency"]:
#     if col in df.columns:
#         df[col] = df[col].fillna("UNKNOWN").astype(str).str.strip()
#         df[col] = df[col].replace({"": "UNKNOWN", "UNK": "UNKNOWN", "nan": "UNKNOWN"})

# st.sidebar.header("Filters")

# if "start_time" in df.columns and df["start_time"].notna().any():
#     min_date = df["start_time"].dt.date.min()
#     max_date = df["start_time"].dt.date.max()
#     selected_dates = st.sidebar.date_input(
#         "Date Range",
#         value=(min_date, max_date),
#         min_value=min_date,
#         max_value=max_date,
#     )
# else:
#     selected_dates = None

# service_filter = st.sidebar.multiselect(
#     "Service Name",
#     sorted(df["Service_Name"].dropna().unique().tolist()) if "Service_Name" in df.columns else []
# )

# receiver_filter = st.sidebar.multiselect(
#     "Receiver Bank",
#     sorted(df["Receiver_Bank"].dropna().unique().tolist()) if "Receiver_Bank" in df.columns else []
# )

# message_filter = st.sidebar.multiselect(
#     "Message Type",
#     sorted(df["Message_Type"].dropna().unique().tolist()) if "Message_Type" in df.columns else []
# )

# risk_filter = st.sidebar.selectbox(
#     "Risk View",
#     ["All Transactions", "Predicted Breaches Only", "Actual Breaches Only"]
# )

# filtered_df = df.copy()

# if selected_dates and isinstance(selected_dates, tuple) and len(selected_dates) == 2 and "start_time" in filtered_df.columns:
#     start_date, end_date = selected_dates
#     filtered_df = filtered_df[
#         filtered_df["start_time"].dt.date.between(start_date, end_date)
#     ]

# if service_filter:
#     filtered_df = filtered_df[filtered_df["Service_Name"].isin(service_filter)]

# if receiver_filter:
#     filtered_df = filtered_df[filtered_df["Receiver_Bank"].isin(receiver_filter)]

# if message_filter:
#     filtered_df = filtered_df[filtered_df["Message_Type"].isin(message_filter)]

# if risk_filter == "Predicted Breaches Only" and "prediction" in filtered_df.columns:
#     filtered_df = filtered_df[filtered_df["prediction"] == 1]

# if risk_filter == "Actual Breaches Only" and "sla_breached" in filtered_df.columns:
#     filtered_df = filtered_df[filtered_df["sla_breached"] == 1]

# # -------------------------
# # Hero section
# # -------------------------
# last_refreshed = datetime.now().strftime("%d %b %Y %H:%M")

# st.markdown(
#     f"""
#     <div class="hero">
#         <div class="hero-title">Payment Operations SLA Intelligence Dashboard</div>
#         <div class="hero-subtitle">Operational reporting, breach monitoring, process analysis, and transaction volume forecasting from payment logs.</div>
#         <div class="hero-meta">
#             <span class="pill pill-blue">SLA Analytics</span>
#             <span class="pill pill-green">Data Source: scored_transactions.csv</span>
#             <span class="pill pill-red">Last viewed: {last_refreshed}</span>
#         </div>
#     </div>
#     """,
#     unsafe_allow_html=True
# )

# # -------------------------
# # Tabs
# # -------------------------
# tab1, tab2, tab3, tab4, tab5 = st.tabs([
#     "Summary",
#     "SLA Reports",
#     "Process Analysis",
#     "Prediction Monitor",
#     "Forecasting"
# ])

# # -------------------------
# # Tab 1: Summary
# # -------------------------
# with tab1:
#     kpis = compute_kpis(filtered_df)

#     st.markdown('<div class="section-title">Executive Summary</div>', unsafe_allow_html=True)

#     c1, c2, c3, c4 = st.columns(4)
#     with c1:
#         render_kpi_card("Total Transactions", f"{kpis['total_transactions']:,}", "Transactions in current filtered view", "blue")
#     with c2:
#         render_kpi_card("SLA Compliance Rate", safe_pct(kpis["sla_compliance_rate"]), "Actual compliance based on historical label", "green")
#     with c3:
#         render_kpi_card("Actual Breaches", f"{kpis['actual_breaches']:,}" if kpis["actual_breaches"] is not None else "N/A", "Historical breach count", "red")
#     with c4:
#         render_kpi_card("Predicted High-Risk", f"{kpis['predicted_breaches']:,}" if kpis["predicted_breaches"] is not None else "N/A", "Transactions classified as likely breach", "orange")

#     c5, c6, c7, c8 = st.columns(4)
#     with c5:
#         render_kpi_card("Avg Processing Minutes", safe_num(kpis["avg_processing_minutes"]), "Mean turnaround time", "blue")
#     with c6:
#         render_kpi_card("Modification Rate", safe_pct(kpis["modification_rate"]), "Transactions requiring modification", "orange")
#     with c7:
#         render_kpi_card("Rework Rate", safe_pct(kpis["rework_rate"]), "Transactions involving rework loops", "orange")
#     with c8:
#         render_kpi_card("Avg Breach Probability", safe_pct(kpis["avg_breach_probability"]), "Model-estimated average risk", "purple")

#     st.markdown('<div class="section-title">Insights</div>', unsafe_allow_html=True)
#     insights = generate_insights(filtered_df)
#     if insights:
#         for insight in insights:
#             st.markdown(f'<div class="insight-box">{insight}</div>', unsafe_allow_html=True)
#     else:
#         st.markdown('<div class="insight-box">No insights available for the current selection.</div>', unsafe_allow_html=True)

#     if "prediction" in filtered_df.columns:
#         st.markdown("#### Predicted Breach Distribution")
#         pred_counts = (
#             filtered_df["prediction"]
#             .value_counts()
#             .rename_axis("prediction")
#             .reset_index(name="count")
#         )
#         pred_counts["prediction_label"] = pred_counts["prediction"].map({
#             0: "Not likely to breach",
#             1: "Likely SLA breach"
#         })

#         fig = px.bar(
#             pred_counts,
#             x="prediction_label",
#             y="count",
#             title="Predicted Breach vs Non-Breach",
#             color="prediction_label",
#             color_discrete_map={
#                 "Likely SLA breach": "#E45756",
#                 "Not likely to breach": "#2E86DE"
#             }
#         )
#         fig = style_plotly(fig)
#         st.plotly_chart(fig, use_container_width=True)

#     if "breach_probability" in filtered_df.columns:
#         st.markdown("#### Breach Probability Distribution")
#         fig = px.histogram(
#             filtered_df,
#             x="breach_probability",
#             nbins=30,
#             title="Breach Probability Histogram"
#         )
#         fig.update_traces(marker_color="#7D3C98")
#         fig = style_plotly(fig)
#         st.plotly_chart(fig, use_container_width=True)

# # -------------------------
# # Tab 2: SLA Reports
# # -------------------------
# with tab2:
#     st.markdown('<div class="section-title">SLA Reporting</div>', unsafe_allow_html=True)

#     month_df = sla_by_month(filtered_df)
#     operator_df = sla_by_operator(filtered_df)
#     service_df = breach_by_service(filtered_df)
#     bank_df = breach_by_receiver_bank(filtered_df)

#     st.markdown("#### SLA by Month")
#     st.dataframe(month_df, use_container_width=True)
#     if not month_df.empty:
#         fig = px.line(
#             month_df,
#             x="year_month",
#             y="sla_compliance_rate",
#             title="SLA Compliance by Month",
#             markers=True
#         )
#         fig.update_traces(line=dict(color="#00A878", width=3))
#         fig = style_plotly(fig)
#         st.plotly_chart(fig, use_container_width=True)

#     st.markdown("#### SLA by Operator")
#     st.dataframe(operator_df, use_container_width=True)
#     if not operator_df.empty:
#         fig = px.bar(
#             operator_df.head(15),
#             x="primary_operator",
#             y="sla_compliance_rate",
#             title="Top Operators by SLA Compliance"
#         )
#         fig.update_traces(marker_color="#2E86DE")
#         fig = style_plotly(fig)
#         st.plotly_chart(fig, use_container_width=True)

#     st.markdown("#### SLA by Service")
#     st.dataframe(service_df, use_container_width=True)
#     if not service_df.empty:
#         fig = px.bar(
#             service_df.head(15),
#             x="Service_Name",
#             y="avg_breach_probability",
#             title="Average Breach Probability by Service"
#         )
#         fig.update_traces(marker_color="#8E44AD")
#         fig = style_plotly(fig)
#         st.plotly_chart(fig, use_container_width=True)

#     st.markdown("#### SLA by Receiver Bank")
#     st.dataframe(bank_df, use_container_width=True)
#     if not bank_df.empty:
#         fig = px.bar(
#             bank_df.head(15),
#             x="Receiver_Bank",
#             y="avg_breach_probability",
#             title="Average Breach Probability by Receiver Bank"
#         )
#         fig.update_traces(marker_color="#16A085")
#         fig = style_plotly(fig)
#         st.plotly_chart(fig, use_container_width=True)

#     d1, d2 = st.columns(2)
#     with d1:
#         st.download_button(
#             "Download SLA by Operator CSV",
#             data=to_csv_bytes(operator_df),
#             file_name="sla_by_operator.csv",
#             mime="text/csv"
#         )
#     with d2:
#         st.download_button(
#             "Download SLA by Month CSV",
#             data=to_csv_bytes(month_df),
#             file_name="sla_by_month.csv",
#             mime="text/csv"
#         )

# # -------------------------
# # Tab 3: Process Analysis
# # -------------------------
# with tab3:
#     st.markdown('<div class="section-title">Process Quality and Rework Analysis</div>', unsafe_allow_html=True)

#     loop_df = loop_count_summary(filtered_df)

#     st.markdown("#### Loop Count Summary")
#     st.dataframe(loop_df, use_container_width=True)

#     if not loop_df.empty:
#         fig = px.bar(
#             loop_df,
#             x="loop_count",
#             y="transaction_count",
#             title="Loop Count Distribution"
#         )
#         fig.update_traces(marker_color="#F39C12")
#         fig = style_plotly(fig)
#         st.plotly_chart(fig, use_container_width=True)

#     if "had_modification" in filtered_df.columns:
#         mod_counts = (
#             filtered_df["had_modification"]
#             .value_counts()
#             .rename_axis("had_modification")
#             .reset_index(name="count")
#         )
#         mod_counts["label"] = mod_counts["had_modification"].map({
#             0: "No Modification",
#             1: "Modified"
#         })

#         fig = px.bar(
#             mod_counts,
#             x="label",
#             y="count",
#             title="Transactions Requiring Modification",
#             color="label",
#             color_discrete_map={
#                 "Modified": "#E67E22",
#                 "No Modification": "#3498DB"
#             }
#         )
#         fig = style_plotly(fig)
#         st.plotly_chart(fig, use_container_width=True)

#     if "n_total_operator_touchpoints" in filtered_df.columns:
#         fig = px.histogram(
#             filtered_df,
#             x="n_total_operator_touchpoints",
#             nbins=20,
#             title="Operator Touchpoint Distribution"
#         )
#         fig.update_traces(marker_color="#7D3C98")
#         fig = style_plotly(fig)
#         st.plotly_chart(fig, use_container_width=True)

#     if "start_dayofweek" in filtered_df.columns and "prediction" in filtered_df.columns:
#         weekday_risk = (
#             filtered_df.groupby("start_dayofweek")["prediction"]
#             .mean()
#             .reset_index(name="predicted_breach_rate")
#         )
#         weekday_map = {
#             0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"
#         }
#         weekday_risk["weekday"] = weekday_risk["start_dayofweek"].map(weekday_map)

#         fig = px.bar(
#             weekday_risk,
#             x="weekday",
#             y="predicted_breach_rate",
#             title="Predicted Breach Rate by Weekday"
#         )
#         fig.update_traces(marker_color="#C0392B")
#         fig = style_plotly(fig)
#         st.plotly_chart(fig, use_container_width=True)

# # -------------------------
# # Tab 4: Prediction Monitor
# # -------------------------
# with tab4:
#     st.markdown('<div class="section-title">Predictive Monitoring</div>', unsafe_allow_html=True)

#     if "breach_probability" in filtered_df.columns:
#         high_risk_df = filtered_df.sort_values("breach_probability", ascending=False).head(50)
#     else:
#         high_risk_df = filtered_df.head(50)

#     st.markdown("#### High-Risk Transactions")
#     display_cols = [
#         c for c in [
#             "Message_Type",
#             "Sender_Bank",
#             "Receiver_Bank",
#             "Service_Name",
#             "Currency",
#             "sla_breached",
#             "prediction",
#             "breach_probability",
#             "interpretation"
#         ] if c in high_risk_df.columns
#     ]
#     st.dataframe(high_risk_df[display_cols] if display_cols else high_risk_df, use_container_width=True)

#     if "start_hour" in filtered_df.columns and "prediction" in filtered_df.columns:
#         hour_risk = (
#             filtered_df.groupby("start_hour")["prediction"]
#             .mean()
#             .reset_index(name="predicted_breach_rate")
#         )
#         fig = px.line(
#             hour_risk,
#             x="start_hour",
#             y="predicted_breach_rate",
#             title="Predicted Breach Rate by Start Hour",
#             markers=True
#         )
#         fig.update_traces(line=dict(color="#E74C3C", width=3))
#         fig = style_plotly(fig)
#         st.plotly_chart(fig, use_container_width=True)

#     if "Receiver_Bank" in filtered_df.columns and "breach_probability" in filtered_df.columns:
#         bank_risk = (
#             filtered_df.groupby("Receiver_Bank")["breach_probability"]
#             .mean()
#             .reset_index()
#             .sort_values("breach_probability", ascending=False)
#             .head(10)
#         )
#         fig = px.bar(
#             bank_risk,
#             x="Receiver_Bank",
#             y="breach_probability",
#             title="Top 10 Receiver Banks by Predicted Risk"
#         )
#         fig.update_traces(marker_color="#E45756")
#         fig = style_plotly(fig)
#         st.plotly_chart(fig, use_container_width=True)

#     st.download_button(
#         "Download Scored Transactions CSV",
#         data=to_csv_bytes(filtered_df),
#         file_name="scored_transactions_filtered.csv",
#         mime="text/csv"
#     )

# # -------------------------
# # Tab 5: Forecasting
# # -------------------------
# with tab5:
#     st.markdown('<div class="section-title">Workload Forecasting</div>', unsafe_allow_html=True)

#     st.info(
#         "Forecast uses Holt-Winters Exponential Smoothing with additive trend and no seasonality. "
#         "Where historical counts are smooth, the forecast may appear close to linear."
#     )

#     try:
#         daily_counts_df = daily_transaction_counts(filtered_df)
#         forecast_df = forecast_transaction_volume(daily_counts_df, periods=7)

#         if not daily_counts_df.empty and not forecast_df.empty:
#             total_hist = int(daily_counts_df["transaction_count"].sum())
#             avg_daily = float(daily_counts_df["transaction_count"].mean())
#             forecast_peak = forecast_df.sort_values("forecast_transaction_count", ascending=False).iloc[0]

#             fc1, fc2, fc3 = st.columns(3)
#             with fc1:
#                 render_kpi_card("Historical Transactions", f"{total_hist:,}", "Observed over selected period", "blue")
#             with fc2:
#                 render_kpi_card("Avg Daily Volume", safe_num(avg_daily), "Average historical daily transactions", "green")
#             with fc3:
#                 render_kpi_card(
#                     "Peak Forecast Day",
#                     safe_num(forecast_peak['forecast_transaction_count']),
#                     f"{forecast_peak['date'].date()}",
#                     "purple"
#                 )

#         st.markdown("#### Historical Daily Transaction Volume")
#         st.dataframe(daily_counts_df, use_container_width=True)

#         fig_hist = px.line(
#             daily_counts_df,
#             x="date",
#             y="transaction_count",
#             title="Historical Daily Transaction Volume",
#             markers=True
#         )
#         fig_hist.update_traces(line=dict(color="#2E86DE", width=3))
#         fig_hist = style_plotly(fig_hist)
#         st.plotly_chart(fig_hist, use_container_width=True)

#         st.markdown("#### Forecasted Transaction Volume (Next 7 Days)")
#         st.dataframe(forecast_df, use_container_width=True)

#         fig = go.Figure()

#         fig.add_trace(go.Scatter(
#             x=daily_counts_df["date"],
#             y=daily_counts_df["transaction_count"],
#             mode="lines+markers",
#             name="Historical",
#             line=dict(color="#2E86DE", width=3)
#         ))

#         fig.add_trace(go.Scatter(
#             x=forecast_df["date"],
#             y=forecast_df["forecast_transaction_count"],
#             mode="lines+markers",
#             name="Forecast",
#             line=dict(color="#E45756", width=3, dash="dash")
#         ))

#         fig.update_layout(title="Transaction Volume: Historical vs Forecast")
#         fig = style_plotly(fig)
#         st.plotly_chart(fig, use_container_width=True)

#     except Exception as e:
#         st.warning(f"Forecasting could not be generated: {e}")