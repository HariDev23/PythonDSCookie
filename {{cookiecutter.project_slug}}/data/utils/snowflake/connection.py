import os
import snowflake.connector
from dotenv import load_dotenv


def init_connection():
    """This function takes the credentials stored as environemnt variables
    and makes the connection with the snowflake database.

    Returns:
        class: Snowflake connection socket
        ('snowflake.connector.connection.SnowflakeConnection')
    """
    load_dotenv()
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        Account_identifier=os.getenv("SNOWFLAKE_ACCOUNT_IDENTIFIER"),
    )
