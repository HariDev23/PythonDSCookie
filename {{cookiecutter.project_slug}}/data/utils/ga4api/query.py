import os
from dotenv import load_dotenv
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric
from google.analytics.data_v1beta.types import RunReportRequest, RunPivotReportRequest, BatchRunReportsRequest, RunRealtimeReportRequest
from google.analytics.data_v1beta.types import Pivot, OrderBy

# Set environment variables
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "client_secrets.json"


def convert_to_pandas(response):
    dim_len = len(response.dimension_headers)
    metric_len = len(response.metric_headers)
    all_data = []
    for row in response.rows:
        row_data = {}
        for i in range(0, dim_len):
            row_data.update(
                {response.dimension_headers[i].name: row.dimension_values[i].value}
            )
        for i in range(0, metric_len):
            row_data.update(
                {response.metric_headers[i].name: row.metric_values[i].value}
            )
        all_data.append(row_data)
    df = pd.DataFrame(all_data)
    return df


def RunReportMethod(property_id):
    """Runs a simple report on a Google Analytics 4 property."""
    # Using a default constructor instructs the client to use the credentials
    # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client = BetaAnalyticsDataClient()

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="city")],
        metrics=[Metric(name="activeUsers")],
        date_ranges=[DateRange(start_date="2022-12-07", end_date="today")],
    )
    response = client.run_report(request)

    # print("Report result:")
    # for row in response.rows:
    #     print(row.dimension_values[0].value, row.metric_values[0].value)
    return convert_to_pandas(response)


def RunPivotMethod(property_id):
    client = BetaAnalyticsDataClient()
    request = RunPivotReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="city"), Dimension(name="browser")],
        metrics=[Metric(name="sessions")],
        date_ranges=[DateRange(start_date="2022-11-15", end_date="today")],
        pivots=[
            Pivot(
                field_names=["city"],
                limit=5,
                order_bys=[
                    OrderBy(
                        dimension=OrderBy.DimensionOrderBy(dimension_name="city")
                    )
                ],
            ),
            Pivot(
                field_names=["browser"],
                offset=0,
                limit=3,
                order_bys=[
                    OrderBy(
                        metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True
                    )
                ],
            ),
        ],
    )
    response = client.run_pivot_report(request)
    return convert_to_pandas(response)

def runBatchReport(property_id):
    """Runs a batch report on a Google Analytics 4 property."""
    # Using a default constructor instructs the client to use the credentials
    # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client = BetaAnalyticsDataClient()

    requests = BatchRunReportsRequest(
        property=f"properties/{property_id}",
        requests = [
      RunReportRequest(
        dimensions=[Dimension(name="city")],
        metrics=[Metric(name="activeUsers")],
        date_ranges=[DateRange(start_date="2022-11-01", end_date="today")]
    ), RunReportRequest(
        dimensions=[Dimension(name="city")],
        metrics=[Metric(name="activeUsers")],
        date_ranges=[DateRange(start_date="2022-11-01", end_date="today")]
    )
    ])
    response = client.batch_run_reports(requests)
    return convert_to_pandas(response)

def RunRealTimereport(property_id):
    """Runs a simple report on a Google Analytics 4 property."""

    # Using a default constructor instructs the client to use the credentials
    # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client = BetaAnalyticsDataClient()

    request = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="city")],
        metrics=[Metric(name="activeUsers")]
    )
    response = client.run_realtime_report(request)
    return convert_to_pandas(response)

if __name__ == "__main__":
    load_dotenv()
    batch = RunRealTimereport(os.getenv('PROPERTY_ID'))
    print(batch)
