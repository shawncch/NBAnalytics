import streamlit as st
from utils.data import get_season_overview

st.set_page_config(page_title="Season Overview", layout="wide")
st.title("🏀 NBA Season Overview")

# ── Filters ──────────────────────────────────────────────
col1, col2 = st.columns([1, 1])

SEASON_IDS = [
    "2025-26", "2024-25", "2023-24", "2022-23", "2021-22", "2020-21",
    "2019-20", "2018-19", "2017-18", "2016-17", "2015-16",
]
SEASON_TYPES = ["regular_season", "post_season"]

with col1:
    season_id = st.selectbox("Season", SEASON_IDS, index=0)

with col2:
    season_type = st.selectbox("Season Type", SEASON_TYPES, index=0)

# ── Data Load ─────────────────────────────────────────────
with st.spinner("Loading standings..."):
    df = get_season_overview(season_id, season_type)

if df.empty:
    st.warning("No data found for this selection.")
    st.stop()

# ── League Standings Table ────────────────────────────────
st.subheader(f"{season_id} · {season_type} Standings")



st.dataframe(
    df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "rnk":           st.column_config.NumberColumn("#", width="small"),
        "team_name":      st.column_config.TextColumn("Team"),
        "team_abbreviation":           st.column_config.TextColumn("Tricode", width="small"),
        "wl_record":         st.column_config.TextColumn("Record", width="small")
    },
)

# # ── Summary KPIs ──────────────────────────────────────────
# st.divider()
# k1, k2, k3 = st.columns(3)
# k1.metric("Teams", len(df))
# k2.metric("Avg PPG", f"{df['pts_per_game'].mean():.1f}")
# k3.metric("Avg Net RTG", f"{df['net_rating'].mean():.1f}")