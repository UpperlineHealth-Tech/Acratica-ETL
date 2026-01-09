import streamlit as st
from snowflake.snowpark.context import get_active_session
import time
import pandas as pd
import snowflake.snowpark.functions as F

st.set_page_config(layout="wide")
st.title("EMPI Approvals UI")

session = get_active_session()

# Variables
LINK_UNLINK_TABLE = "upperline_refined.dbt_empi_stg.empi_link_unlink_overrides"
SQL = f"""
SELECT *
FROM {LINK_UNLINK_TABLE}
"""

# Function definitions
def load_df():
    return session.sql(SQL).to_pandas().reset_index(drop=True)

def action(df, approval_comment, status, link_unlink_table = LINK_UNLINK_TABLE):
    target = session.table(link_unlink_table)

    if isinstance(df, pd.DataFrame):
        if df.empty:
            return  # nothing to do
        df = session.create_dataframe(df[["ID"]].drop_duplicates())

    result = target.merge(
        source = df,
        join_expr = target["ID"] == df["ID"],
        clauses = [
            F.when_matched().update({
                "APPROVAL_STATUS": status,
                "REVIEWED_BY": F.current_user(),
                "REVIEWER_COMMENT": approval_comment,
                "REVIEWED_AT": F.current_timestamp()
            })
        ]
    )
    return result    

# State variables
if "approval_comment" not in st.session_state:
    st.session_state["approval_comment"] = ""
if "clear_approval_comment" not in st.session_state:
    st.session_state["clear_approval_comment"] = False

if st.session_state["clear_approval_comment"]:
    st.session_state["approval_comment"] = ""
    st.session_state["clear_approval_comment"] = False

# Nonce forces Streamlit to treat the dataframe widget as "new" (clears selection UI)
if "df_key_nonce" not in st.session_state:
    st.session_state.df_key_nonce = 0

if "base_df" not in st.session_state:
    st.subheader("Selecting rows from the overrides table ...")
    st.session_state.base_df = load_df()

base_df = st.session_state.base_df

# Layout
col1, col2 = st.columns(2)

with col1:
    if st.button("Clear selections"):
        st.session_state.df_key_nonce += 1
        st.rerun()

with col2:
    if st.button("Reload sample"):
        st.session_state.base_df = load_df()
        st.session_state.df_key_nonce += 1
        st.rerun()

st.subheader("Select rows (click rows / use left checkboxes)")

event = st.dataframe(
    base_df,
    key=f"approval_df_{st.session_state.df_key_nonce}",
    width= "stretch",
    hide_index=True,
    selection_mode="multi-row",
    on_select="rerun",
)

selected_row_idxs = event.selection.rows if event and event.selection else []
selected_df = base_df.iloc[selected_row_idxs].copy()

st.subheader("Selected rows")
st.write("Selected rows:", len(selected_df))
st.dataframe(selected_df, width="stretch", hide_index=True)

approval_comment = st.text_area(
    "Comment for this approval (required), press Enter when done",
    key="approval_comment",
    placeholder="E.g., confirmed duplicate after manual review."
)

c1, c2, c3 = st.columns(3)

if c1.button("Submit Approval"):
    if not st.session_state["approval_comment"].strip():
        st.error("Please enter an approval comment.")
        st.stop()

    try:
        if len(selected_df) < 1:
            raise ValueError("No rows selected for approval.")
        if selected_df['APPROVAL_STATUS'].eq('Y').any():
            raise ValueError("Some selected rows have already been approved.")
        action(selected_df, st.session_state["approval_comment"], 'Y')
        st.success(f"Done! Rows approved: {len(selected_df)}")
        time.sleep(1)

        # clear reason to prevent reuse
        st.session_state["clear_approval_comment"] = True
        st.session_state["df_key_nonce"] += 1
        st.rerun()

    except Exception as e:
        st.error(f"Error linking records: {e}")

if c2.button("Remove Approval"):
    if not st.session_state["approval_comment"].strip():
        st.error("Please enter an approval comment.")
        st.stop()

    try:
        if len(selected_df) < 1:
            raise ValueError("No rows selected to remove approval.")
        if selected_df['APPROVAL_STATUS'].eq('N').any():
            raise ValueError("Some selected rows are not approved.")
        action(selected_df, st.session_state["approval_comment"], 'N')
        st.success(f"Done! Rows unapproved: {len(selected_df)}")
        time.sleep(1)

        # clear reason to prevent reuse
        st.session_state["clear_approval_comment"] = True
        st.session_state["df_key_nonce"] += 1
        st.rerun()

    except Exception as e:
        st.error(f"Error linking records: {e}")

if c3.button("Cancel"):
    st.success(f"Transaction canceled!")
    time.sleep(1)

    # clear reason to prevent reuse
    st.session_state["clear_approval_comment"] = True
    st.session_state["df_key_nonce"] += 1
    st.rerun()