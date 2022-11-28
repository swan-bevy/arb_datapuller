# =============================================================================
# IMPORTS
# =============================================================================
import os, boto3

# =============================================================================
# CONSTANTS
# =============================================================================
BUCKET_NAME = "arb-live-data"
FTX_US_BASEURL = "https://ftx.us/api/markets/"
FTX_GLOBAL_BASEURL = "https://ftx.com/api/markets/"
DYDX_BASEURL = "https://api.dydx.exchange"  # NO "/" AT THE END!!!
OKX_BASEURL = "https://www.okx.com/"
BINANCE_US_BASEURL = "https://api.binance.us/api/v3/depth?"
COINBASE_BASEURL = "https://api.exchange.coinbase.com/products/"
DISCORD_URL = "https://discord.com/api/webhooks/1028097581303205999/1UtTckX8MRHY9JwY4IibOL_syhB7mXEKUysNF3ZUxrHwK05vY77lyeGNUCPvIPvSovZj"
SECS_PER_HOUR = 60 * 60

# =============================================================================
# AWS CONFIG
# =============================================================================
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name="eu-central-1",
)
