import streamlit as st
from snowflake.snowpark.context import get_active_session
import time
import pandas as pd
import snowflake.snowpark.functions as F
import hashlib


st.set_page_config(layout="wide")
st.title("EMPI Link / Unlink UI")

session = get_active_session()

# Variables
XWALK_TABLE = "upperline_sandbox.stg_acratica.det_empi_crosswalk_shadow"
LINK_UNLINK_TABLE = "upperline_sandbox.stg_acratica.empi_link_unlink_overrides"
KEY_COL = "SURROGATE_KEY"

# Function definitions
def load_df(search_text: str = '', limit: int = 1000, xwalk_table = XWALK_TABLE):
    t = session.table(xwalk_table)

    s = (search_text or "").strip()

    if s:
        pat = f"%{s.upper()}%"

        t = t.filter(
            F.upper(F.col("SURROGATE_KEY")).like(pat) |
            F.upper(F.col("SOURCE_SYSTEM_ID")).like(pat) |
            F.upper(F.col("EMPI_ID")).like(pat) |
            F.upper(F.col("FIRST_NAME")).like(pat) |
            F.upper(F.col("LAST_NAME")).like(pat) |
            F.upper(F.col("SOURCE_SYSTEM_ID_2")).like(pat)
        )

    t = t.sort(F.col("CLUSTER_UPDATED_AT").desc())
    
    return t.limit(limit).to_pandas().reset_index(drop=True)

def link_candidates(df, xwalk_table):
    if isinstance(df, pd.DataFrame):
        affected_empi_ids = df["EMPI_ID"].astype(str).unique().tolist()
    else:
        affected_empi_ids = [row[0] for row in df.select("EMPI_ID").distinct().collect()]

    if len(affected_empi_ids) < 2:
        raise ValueError("Please select at least two distinct empi_ids to link.")
  
    placeholders = ", ".join(["?"]*len(affected_empi_ids))
    df_affected = session.sql(f"""
    SELECT *
    FROM {xwalk_table}
    WHERE EMPI_ID IN ({placeholders})""", params=affected_empi_ids)

    min_ssid = df_affected.select(F.min("SOURCE_SYSTEM_ID")).collect()[0][0]
    new_empi_id = df_affected \
        .filter(F.col("SOURCE_SYSTEM_ID") == min_ssid) \
        .select(F.col("EMPI_ID")) \
        .distinct() \
        .collect()[0][0]
    return df_affected, new_empi_id

def link_records(df, reason, link_unlink_table=LINK_UNLINK_TABLE, xwalk_table=XWALK_TABLE):
    df_affected, new_empi_id = link_candidates(df, xwalk_table)

    df_to_insert = df_affected.select(
        F.col("SURROGATE_KEY"),
        F.col("SOURCE_SYSTEM"),
        F.col("SOURCE_SYSTEM_ID"),
        F.col("SOURCE_SYSTEM_ID_2"),
        F.col("EMPI_ID"),
        F.lit(new_empi_id).alias("NEW_EMPI_ID"),
        F.lit("LINK").alias("OVERRIDE_ACTION"),
        F.lit(reason).alias("REASON")
    )

    df_to_insert.write.save_as_table(link_unlink_table, mode="append", column_order="name")

def unlink_records(df, reason, link_unlink_table=LINK_UNLINK_TABLE, xwalk_table=XWALK_TABLE):
    if isinstance(df, pd.DataFrame):
        affected_sk = df["SURROGATE_KEY"].astype(str).unique().tolist()
    else:
        affected_sk = [row[0] for row in df.select("SURROGATE_KEY").distinct().collect()]

    if len(affected_sk) != 1:
        raise ValueError("Please select exactly one record to unlink.")
    
    new_empi_id = hashlib.sha256(str(affected_sk).encode("utf-8")).hexdigest()[:32]
    
    placeholders = ", ".join(["?"]*len(affected_sk))
    df_affected = session.sql(f"""
    SELECT *
    FROM {xwalk_table}
    WHERE SURROGATE_KEY IN ({placeholders})""", params=affected_sk)      
    
    
    df_to_insert = df_affected.select(
        F.col("SURROGATE_KEY"),
        F.col("SOURCE_SYSTEM"),
        F.col("SOURCE_SYSTEM_ID"),
        F.col("SOURCE_SYSTEM_ID_2"),
        F.col("EMPI_ID"),
        F.lit(new_empi_id).alias("NEW_EMPI_ID"),
        F.lit("UNLINK").alias("OVERRIDE_ACTION"),
        F.lit(reason).alias("REASON")
    )

    df_to_insert.write.save_as_table(link_unlink_table, mode="append", column_order="name")

# State variables
if "reason" not in st.session_state:
    st.session_state["reason"] = ""
if "clear_reason" not in st.session_state:
    st.session_state["clear_reason"] = False

if st.session_state["clear_reason"]:
    st.session_state["reason"] = ""
    st.session_state["clear_reason"] = False

# Nonce forces Streamlit to treat the dataframe widget as "new" (clears selection UI)
if "df_key_nonce" not in st.session_state:
    st.session_state.df_key_nonce = 0

if "selected_df" not in st.session_state:
    st.session_state.selected_df = pd.DataFrame()

st.subheader("Find records")

cs1, cs2 = st.columns(2)
do_search = cs1.button("Search")
do_reset = cs2.button("Reset")

search_text = st.text_input(
    "Search (surrogate_key / source_system_id / source_system_id_2 / empi_id / first_name / last_name)",
    key="search_text",
    placeholder="Type an ID and click Search…"
)

if "base_df" not in st.session_state:
    st.subheader("Selecting rows from the xwalk table ...")
    # safe default: load only a small sample unless they search
    st.session_state.base_df = load_df(search_text="", limit=500)

if do_search:
    st.session_state.base_df = load_df(search_text=st.session_state["search_text"], limit=1000)
    st.session_state.df_key_nonce += 1
    st.rerun()

if do_reset:
    st.session_state.base_df = load_df(search_text="", limit=500)
    st.session_state.df_key_nonce += 1
    st.rerun()

st.write(f"Search returned {len(st.session_state.base_df)} records")

# Layout
base_df = st.session_state.base_df

st.subheader("Select rows (click rows / use left checkboxes)")

col1, col2 = st.columns(2)

with col1:
    if st.button("Clear selections"):
        st.session_state.df_key_nonce += 1
        st.rerun()

with col2:
    if st.button("Reload sample"):
        st.session_state.base_df = load_df(search_text="", limit=500)
        st.session_state.df_key_nonce += 1
        st.rerun()

# ----- Row selection on st.dataframe -----
event = st.dataframe(
    base_df,
    key=f"xwalk_df_{st.session_state.df_key_nonce}",
    width= "stretch",
    hide_index=True,
    selection_mode="multi-row",
    on_select="rerun",
)


selected_row_idxs = event.selection.rows if event and event.selection else []
cur_selected = base_df.iloc[selected_row_idxs].copy()

c_add, c_clear = st.columns(2)

if c_add.button("Add highlighted rows to Selected"):
    if cur_selected.empty:
        st.warning("Highlight at least one row in the table first.")
    else:
        if st.session_state.selected_df.empty:
            st.session_state.selected_df = cur_selected.copy()
        else:
            st.session_state.selected_df = (
                pd.concat([st.session_state.selected_df, cur_selected], ignore_index=True)
                .drop_duplicates(subset=[KEY_COL])
                .reset_index(drop=True)
            )

        # clear highlight UI in the top table
        st.session_state.df_key_nonce += 1
        st.rerun()

if c_clear.button("Clear Selected"):
    st.session_state.selected_df = pd.DataFrame()
    st.session_state.df_key_nonce += 1
    st.rerun()


st.subheader("Selected rows")
st.write("Selected rows:", len(st.session_state.selected_df))
st.dataframe(st.session_state.selected_df, width="stretch", hide_index=True)

reason = st.text_area(
    "Reason for this change (required), press Enter when done",
    key="reason",
    placeholder="E.g., confirmed duplicate after manual review."
)

c1, c2 = st.columns(2)

if c1.button("Link records"):
    if not st.session_state["reason"].strip():
        st.error("Please enter a reason.")
        st.stop()

    try:
        df_to_process = st.session_state.selected_df
        if df_to_process.empty:
            st.error("No rows selected. Add rows to Selected first.")
            st.stop()
        link_records(df_to_process, st.session_state["reason"])
        st.success(f"Done! Rows linked: {len(df_to_process)}")
        time.sleep(1)

        # clear reason to prevent reuse
        st.session_state["clear_reason"] = True
        st.session_state["df_key_nonce"] += 1
        st.rerun()

    except Exception as e:
        st.error(f"Error linking records: {e}")

if c2.button("Unlink records"):
    if not st.session_state["reason"].strip():
        st.error("Please enter a reason.")
        st.stop()
    
    try:
        df_to_process = st.session_state.selected_df
        if df_to_process.empty:
            st.error("No rows selected. Add rows to Selected first.")
            st.stop()
        unlink_records(df_to_process, st.session_state["reason"])
        st.success(f"Done! Rows unlinked: {len(df_to_process)}")
        time.sleep(1)

        # clear reason to prevent reuse
        st.session_state["clear_reason"] = True
        st.session_state["df_key_nonce"] += 1
        st.rerun()

    except Exception as e:
        st.error(f"Error unlinking records: {e}")
