import streamlit as st
import altair as alt
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import pandas as pd


st.title('* Customer Returns Insights')

#estbalish snowfalke session
@st.cache_resource
def create_session():
    return Session.builder.configs(st.secrets.snowflake).create()

session = create_session()

if session is not None:
    st.success("Connected to Snowflake!")
else:
    st.error("Failed to connect to Snowflake.")


#Load data table
@st.cache_data
def load_data(table_name, account_name):
    ##read in datat table
    st.write(f"Here's a table showing the different reasons for customers returning their products from {account_name}:")
    table = session.table(table_name).filter(col('ACCOUNT_NAME') == account_name)
    ## collect the resilts. this will run the query and download the data
    table = table.select(col('REASON')).collect()
    return table


st.subheader('Reasons for returning shoes')
def load_reasons(table_name, account_table):
    if account_table:
        df = pd.DataFrame(account_table, columns=["REASON"])

        chart = alt.Chart(df).mark_bar().encode(
            x=alt.X('count():Q', title='Count'),
            y=alt.Y('REASON:N', title='Reason', axis=alt.Axis(labelAlign='right', labelFontSize=15, labelLimit=0))
        ).properties(
            width=800,
            height=600
        )

        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("No data to display.")


table_name = 'GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA'
with st.expander("See table for Big Peach"):
    df_Big_Peach = load_data(table_name, 'Big Peach')
    st.write(load_reasons(table_name,df_Big_Peach))

with st.expander("See table for Smart Brand Labs"):
    df_sbl = load_data(table_name, 'SBL')
    st.write(load_reasons(table_name, df_sbl))

with st.expander("See table for Salsitas Mendoza"):
    df_SM = load_data(table_name, 'Salsitas Mendoza')
    st.write(load_reasons(table_name, df_SM))
