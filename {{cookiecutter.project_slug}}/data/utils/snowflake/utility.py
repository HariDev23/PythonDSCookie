import pandas as pd
import db.snowflake.connection as db
import streamlit as st


@st.cache(suppress_st_warning=True, ttl=36000)
def data_fetcher():
    """This function fetches the data based on the given query
    from the Snowflake database.

    Returns:
     pandas.DataFrame: Dataset consisting the details of the
     customer churns
    """
    conn = db.init_connection()
    query = """SELECT * FROM bankcurners"""
    cur = conn.cursor().execute(query)
    snowflake_data = cur.fetch_pandas_all()
    return snowflake_data


def log_fetcher():
    """This function fetches the prediction log data based on the
    given query from the Snowflake database.

    Returns:
     pandas.DataFrame: Prediction history dataset
    """
    conn = db.init_connection()
    query = """SELECT TOP 5 * FROM MODEL_DATA"""
    cur = conn.cursor().execute(query)
    snowflake_data = pd.DataFrame.from_records(
        iter(cur), columns=[x[0] for x in cur.description]
    )
    return snowflake_data


def load_data(
    customer_age,
    dependent_count,
    total_relationship_count,
    months_inactive,
    total_revolving_bal,
    total_trans_amt,
    total_trans_ct,
    avg_util_ratio,
    ml_prediction,
):
    """This function loads the filter values and predicted value
    back to another snowflake db which stores the prediction
    history.

    Args:
        customer_age (int): age of the customer
        dependent_count (int): dependent count
        total_relationship_count (int): relationship count
        months_inactive (int): credit card inactive months
        total_revolving_bal (float): total revolving balance
        total_trans_amt (float): total transaction amount
        total_trans_ct (int): total transaction count
        avg_util_ratio (float): average utilization ratio
        ml_prediction (int): predicted value
    """
    conn = db.init_connection()
    # If the prediction is 0 then he is a existing customer and vice-versa.
    if ml_prediction == 0:
        prediction_str = "Existing"
    else:
        prediction_str = "Attritred"
    # Inserting the filter values and predicted value to the snowflake db.
    conn.cursor().execute(
        """ INSERT INTO MODEL_DATA VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s )
        """,
        (
            customer_age,
            dependent_count,
            total_relationship_count,
            months_inactive,
            total_revolving_bal,
            total_trans_amt,
            total_trans_ct,
            avg_util_ratio,
            prediction_str,
        ),
    )
