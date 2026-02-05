import pandas as pd
import streamlit as st
import snowflake.connector
import plotly.express as px
import os
from dotenv import load_dotenv

load_dotenv()

def get_snowflake_connection():
    """
    Connects to Snowflake environment.
    """

    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema='ANALYTICS'
    )

st.set_page_config(page_title = 'OpenLedger Monitor', layout = 'wide')
st.title('OpenLedger: Reconciliation & Fraud Monitor')
st.markdown('---')

try:
    conn = get_snowflake_connection()
    query_status = """
        SELECT reconciliation_status, COUNT(*) AS count, SUM(ABS(reconciliation_delta)) AS total_delta
        FROM fct_transactions
        GROUP BY reconciliation_status
    """
    df_status = pd.read_sql(query_status, conn)

    query_daily = """
        SELECT DATE(transaction_at) AS TX_DATE, COUNT(*) AS TX_COUNT, SUM(CASE WHEN reconciliation_status != 'MATCHED' THEN 1 ELSE 0 END) AS BREAK_COUNT
        FROM fct_transactions
        GROUP BY DATE(transaction_at)
        ORDER BY TX_DATE ASC
    """
    df_daily = pd.read_sql(query_daily, conn)
    df_daily.columns = df_daily.columns.str.upper()
    df_daily['TX_DATE'] = pd.to_datetime(df_daily['TX_DATE'])
    df_daily = df_daily.sort_values(by = 'TX_DATE')
    df_daily.set_index('TX_DATE', inplace = True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader('Reconciliation Status')
        pie_chart = px.pie(
            df_status,
            values = 'COUNT',
            names = 'RECONCILIATION_STATUS',
            title = 'Transaction Match Rates',
            hole = 0.5,
            color_discrete_sequence = px.colors.qualitative.Pastel
        )
        st.plotly_chart(pie_chart, use_container_width = True)

    with col2:
        st.subheader('Discrepancy Breakdown')
        st.write('Failed Transactions for Reconciliation')
        df_breaks = df_status[df_status['RECONCILIATION_STATUS'] != 'MATCHED']
        st.dataframe(df_breaks, use_container_width = True)
    
    st.subheader('Daily Transaction Volume vs. Breaks')

    st.line_chart(
        df_daily[['TX_COUNT', 'BREAK_COUNT']]
    )

except Exception as exception:
    st.error(f'Snowflake connection has failed: {exception}.')