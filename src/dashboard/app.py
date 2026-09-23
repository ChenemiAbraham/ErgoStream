"""Real-time ErgoStream dashboard using Streamlit."""

import streamlit as st
import json
import time
from datetime import datetime
from collections import deque
from confluent_kafka import Consumer
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.utils import settings, log
from src.models import RiskLevel


# Page config
st.set_page_config(
    page_title="ErgoStream - Live Monitoring",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .big-metric {
        font-size: 2.5rem;
        font-weight: bold;
    }
    .risk-high {
        color: #ff4444;
    }
    .risk-moderate {
        color: #ffaa00;
    }
    .risk-low {
        color: #44ff44;
    }
</style>
""", unsafe_allow_html=True)


class DashboardState:
    """Maintain dashboard state across reruns."""

    def __init__(self):
        if 'events' not in st.session_state:
            st.session_state.events = deque(maxlen=100)
        if 'risk_events' not in st.session_state:
            st.session_state.risk_events = deque(maxlen=50)
        if 'interventions' not in st.session_state:
            st.session_state.interventions = deque(maxlen=30)
        if 'last_update' not in st.session_state:
            st.session_state.last_update = datetime.now()

    @property
    def events(self):
        return st.session_state.events

    @property
    def risk_events(self):
        return st.session_state.risk_events

    @property
    def interventions(self):
        return st.session_state.interventions


def init_kafka_consumer(topic: str, group_id: str) -> Consumer:
    """Initialize Kafka consumer."""
    consumer_config = settings.kafka_config.copy()
    consumer_config.update({
        'group.id': group_id,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
    })
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])
    return consumer


def poll_events(consumer: Consumer, max_messages: int = 10):
    """Poll events from Kafka (non-blocking)."""
    messages = []
    for _ in range(max_messages):
        msg = consumer.poll(timeout=0.1)
        if msg is None:
            break
        if msg.error():
            continue

        try:
            value = json.loads(msg.value().decode('utf-8'))
            messages.append(value)
        except Exception as e:
            log.error(f"Error parsing message: {e}")

    return messages


def render_header():
    """Render dashboard header."""
    col1, col2 = st.columns([3, 1])

    with col1:
        st.title("🏗️ ErgoStream")
        st.markdown("**Real-time human intelligence for the physical workplace**")

    with col2:
        st.markdown(f"""
        <div style='text-align: right; padding-top: 1rem;'>
            <span style='color: #666;'>Last updated:</span><br>
            <strong>{datetime.now().strftime('%H:%M:%S')}</strong>
        </div>
        """, unsafe_allow_html=True)


def render_metrics(state: DashboardState):
    """Render key metrics."""
    col1, col2, col3, col4 = st.columns(4)

    # Active workers
    active_workers = len(set(e.get('worker_id') for e in state.events if e))

    # High risk count
    high_risk_count = sum(
        1 for e in state.risk_events
        if e.get('risk_level') in ['HIGH', 'CRITICAL']
    )

    # Total interventions
    total_interventions = len(state.interventions)

    # Average risk score
    risk_scores = [e.get('risk_score', 0) for e in state.risk_events if e.get('risk_score')]
    avg_risk = sum(risk_scores) / len(risk_scores) if risk_scores else 0

    with col1:
        st.metric("Active Workers", active_workers, delta=None)

    with col2:
        st.metric("High Risk Alerts", high_risk_count, delta=None)

    with col3:
        st.metric("Interventions", total_interventions, delta=None)

    with col4:
        st.metric("Avg Risk Score", f"{avg_risk:.1f}", delta=None)


def render_live_risk_map(state: DashboardState):
    """Render live risk map by station."""
    st.subheader("📍 Live Station Risk Map")

    if not state.risk_events:
        st.info("Waiting for risk events...")
        return

    # Get latest risk by station
    station_risks = {}
    for event in reversed(list(state.risk_events)):
        station = event.get('station_id')
        if station and station not in station_risks:
            station_risks[station] = event

    # Create visualization
    stations = sorted(station_risks.keys())
    risk_scores = [station_risks[s].get('risk_score', 0) for s in stations]
    risk_levels = [station_risks[s].get('risk_level', 'LOW') for s in stations]

    # Color mapping
    colors = {
        'CRITICAL': '#ff0000',
        'HIGH': '#ff4444',
        'MODERATE': '#ffaa00',
        'LOW': '#44ff44'
    }
    bar_colors = [colors.get(level, '#cccccc') for level in risk_levels]

    fig = go.Figure(data=[
        go.Bar(
            x=stations,
            y=risk_scores,
            marker_color=bar_colors,
            text=risk_levels,
            textposition='auto',
        )
    ])

    fig.update_layout(
        height=300,
        margin=dict(l=0, r=0, t=20, b=0),
        xaxis_title="Station",
        yaxis_title="Risk Score",
        yaxis_range=[0, 100],
        showlegend=False
    )

    st.plotly_chart(fig, use_container_width=True)


def render_recent_alerts(state: DashboardState):
    """Render recent high-risk alerts."""
    st.subheader("🚨 Recent High-Risk Alerts")

    high_risk = [
        e for e in state.risk_events
        if e.get('risk_level') in ['HIGH', 'CRITICAL']
    ]

    if not high_risk:
        st.success("✅ No high-risk alerts")
        return

    # Show last 5
    for event in list(reversed(high_risk))[:5]:
        risk_level = event.get('risk_level', 'UNKNOWN')
        worker_id = event.get('worker_id', 'Unknown')
        station = event.get('station_id', 'Unknown')
        risk_score = event.get('risk_score', 0)
        primary_risk = event.get('primary_risk', 'Unknown')

        # Color based on risk
        emoji = "🔴" if risk_level == "CRITICAL" else "🟠"
        color = "risk-high" if risk_level == "CRITICAL" else "risk-moderate"

        with st.container():
            col1, col2, col3 = st.columns([2, 3, 2])

            with col1:
                st.markdown(f"{emoji} **{worker_id}** @ {station}")

            with col2:
                st.markdown(f"*{primary_risk}*")

            with col3:
                st.markdown(f"<span class='{color}'>Score: {risk_score:.1f}</span>", unsafe_allow_html=True)

            st.markdown("---")


def render_interventions(state: DashboardState):
    """Render AI-generated interventions."""
    st.subheader("💡 AI Interventions")

    if not state.interventions:
        st.info("No interventions generated yet")
        return

    # Show last 3 interventions
    for intervention in list(reversed(state.interventions))[:3]:
        worker_id = intervention.get('worker_id', 'Unknown')
        immediate_action = intervention.get('immediate_action', '')
        recommendations = intervention.get('recommendations', [])

        with st.expander(f"🤖 {worker_id} - {immediate_action}", expanded=False):
            st.markdown("**Recommendations:**")
            for rec in recommendations:
                st.markdown(f"- {rec}")

            rationale = intervention.get('rationale', '')
            if rationale:
                st.markdown(f"**Rationale:** {rationale}")


def render_risk_timeline(state: DashboardState):
    """Render risk score timeline."""
    st.subheader("📈 Risk Score Timeline")

    if len(state.risk_events) < 2:
        st.info("Collecting data...")
        return

    # Convert to DataFrame
    df = pd.DataFrame(list(state.risk_events))
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Create timeline plot
    fig = go.Figure()

    for worker_id in df['worker_id'].unique():
        worker_data = df[df['worker_id'] == worker_id]

        fig.add_trace(go.Scatter(
            x=worker_data['timestamp'],
            y=worker_data['risk_score'],
            mode='lines+markers',
            name=worker_id,
            line=dict(width=2),
            marker=dict(size=6)
        ))

    fig.update_layout(
        height=300,
        margin=dict(l=0, r=0, t=20, b=0),
        xaxis_title="Time",
        yaxis_title="Risk Score",
        yaxis_range=[0, 100],
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    st.plotly_chart(fig, use_container_width=True)


def main():
    """Main dashboard application."""
    render_header()

    # Initialize state
    state = DashboardState()

    # Initialize consumers (with caching)
    @st.cache_resource
    def get_consumers():
        motion_consumer = init_kafka_consumer(settings.topic_worker_motion, 'dashboard-motion')
        risk_consumer = init_kafka_consumer(settings.topic_risk_detected, 'dashboard-risk')
        intervention_consumer = init_kafka_consumer(settings.topic_interventions, 'dashboard-interventions')
        return motion_consumer, risk_consumer, intervention_consumer

    try:
        motion_consumer, risk_consumer, intervention_consumer = get_consumers()

        # Poll for new events
        motion_events = poll_events(motion_consumer, max_messages=20)
        risk_events = poll_events(risk_consumer, max_messages=10)
        intervention_events = poll_events(intervention_consumer, max_messages=5)

        # Update state
        for event in motion_events:
            state.events.append(event)

        for event in risk_events:
            state.risk_events.append(event)

        for event in intervention_events:
            state.interventions.append(event)

        # Render metrics
        render_metrics(state)

        st.markdown("---")

        # Main content area
        col1, col2 = st.columns([2, 1])

        with col1:
            render_live_risk_map(state)
            render_risk_timeline(state)

        with col2:
            render_recent_alerts(state)

        st.markdown("---")

        # Interventions
        render_interventions(state)

        # Auto-refresh
        time.sleep(2)
        st.rerun()

    except Exception as e:
        st.error(f"Error: {e}")
        log.error(f"Dashboard error: {e}")


if __name__ == "__main__":
    main()
