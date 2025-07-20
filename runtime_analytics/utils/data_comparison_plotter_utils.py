from __future__ import annotations

from datetime import date
import pandas as pd
import plotly.graph_objects as go


def create_duration_comparison_chart(
    df: pd.DataFrame,
    selected_date: date,
    reference_date: date
) -> go.Figure:
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=df["type"],
            y=df["duration_current_min"],
            name=selected_date.strftime('%d-%b-%Y'),
            marker_color="royalblue",
            hovertext=[f"Jobs: {jc}" for jc in df["job_count"]],
            hoverinfo="text+y",
        )
    )
    fig.add_trace(
        go.Bar(
            x=df["type"],
            y=df["duration_ref_min"],
            name=reference_date.strftime('%d-%b-%Y'),
            marker_color="lightcoral",
        )
    )

    for _, row in df.iterrows():
        delta = row["delta"]
        symbol = "🔺" if delta > 0 else "🔻"
        color = "red" if delta > 0 else "green"
        fig.add_annotation(
            x=row["type"],
            y=max(row["duration_current_min"], row["duration_ref_min"]) + 1,
            text=f"<span style='color:{color}'>{symbol} {delta:.1f}</span>",
            showarrow=False,
            yshift=10,
        )

    fig.update_layout(
        barmode="group",
        xaxis_title="Job Type",
        yaxis_title="Duration (minutes)",
        title="Duration Comparison by Job Type",
        legend_title="Run Date",
    )

    return fig


def create_drilldown_chart(
    pivot_df: pd.DataFrame,
    selected_date: date,
    reference_date: date,
    job_type: str
) -> go.Figure:
    fig = go.Figure()

    for date_val in [reference_date, selected_date]:
        if date_val in pivot_df.columns:
            hover_texts = [
                f"ID: {row['id']}<br>Config Count: {row['config_count']}<br>Date: {date_val.strftime('%d-%b-%Y')}"
                for _, row in pivot_df.iterrows()
            ]
            fig.add_trace(
                go.Bar(
                    x=pivot_df["id"],
                    y=pivot_df[date_val],
                    name=date_val.strftime("%d-%b-%Y"),
                    hovertext=hover_texts,
                    hoverinfo="text",
                )
            )

    for _, row in pivot_df.iterrows():
        delta = row["delta"]
        symbol = "🔺" if delta > 0 else "🔻"
        color = "red" if delta > 0 else "green"
        max_y = max(row.get(selected_date, 0), row.get(reference_date, 0))
        fig.add_annotation(
            x=row["id"],
            y=max_y + 1,
            text=f"<span style='color:{color}'>{symbol} {abs(delta):.1f}</span>",
            showarrow=False,
            yshift=8,
        )

    fig.update_layout(
        title=f"Duration by ID – {job_type}",
        xaxis_title="ID",
        yaxis_title="Duration (minutes)",
        barmode="group",
        legend_title="Run Date",
    )

    return fig