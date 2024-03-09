# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col,when_matched
import requests


# Write directly to the app
st.title(" :cup_with_straw: Pending Smoothie Orders :cup_with_straw: ")
st.write(
    """Orders That need to be filled
    """
)

# Get the current credentials
#session = get_active_session()
cnx = st.connection("snowflake")
session = cnx.session()

my_dataframe = session.table("smoothies.public.orders").filter(col("order_filled")==0).collect()
#st.dataframe(data=my_dataframe, use_container_width=True)
editable_df = st.experimental_data_editor(my_dataframe)

submitted = st.button('Submit')

if submitted :
    og_dataset = session.table("smoothies.public.orders")
    edited_dataset = session.create_dataframe(editable_df)
    try:
        og_dataset.merge(edited_dataset
        , (og_dataset['order_uid'] == edited_dataset['order_uid'])
        , [when_matched().update({'order_filled': edited_dataset['order_filled']})]
        )
        st.success("Orders Updated")
    except:
        st.write("Something went wrong")


else:
        st.write("There is no Shit! pending")
