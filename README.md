# Purpose

The arb datapuller allows you to concurrently fetch crypto price data from various exchanges. The data is fetched a variable intervals and aggregated into a large dataframe. At the end of the UTC day, the data is saved to S3 and the dataframe resets.
