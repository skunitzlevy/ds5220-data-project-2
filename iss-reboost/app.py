import io
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import requests
import seaborn as sns
from boto3.dynamodb.conditions import Key

matplotlib.use("Agg")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

WEATHER_API_URL = os.environ.get(
    "WEATHER_API_URL",
    (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=32.76&longitude=-79.83"
        "&current=temperature_2m,apparent_temperature,wind_speed_10m,precipitation"
        "&forecast_days=1&temperature_unit=fahrenheit"
    ),
)
SOURCE_ID = os.environ.get("SOURCE_ID", "charleston-weather")
TABLE_NAME = os.environ["DYNAMODB_TABLE"]
S3_BUCKET = os.environ["S3_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
PLOT_KEY = "weather-timeseries.png"


def fetch_weather() -> dict:
    """Fetch the current weather snapshot and return a DynamoDB-ready item."""
    resp = requests.get(WEATHER_API_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    current = data["current"]

    temperature = Decimal(str(current["temperature_2m"]))
    apparent = Decimal(str(current["apparent_temperature"]))
    wind_speed = Decimal(str(current["wind_speed_10m"]))
    precipitation = Decimal(str(current["precipitation"]))

    return {
        "source_id": SOURCE_ID,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "api_time": current["time"],
        "temperature_2m": temperature,
        "apparent_temperature": apparent,
        "temp_gap_f": temperature - apparent,
        "wind_speed_10m": wind_speed,
        "precipitation": precipitation,
        "latitude": Decimal(str(data["latitude"])),
        "longitude": Decimal(str(data["longitude"])),
    }


def fetch_history(table) -> pd.DataFrame:
    """Return all stored weather records as a DataFrame sorted by timestamp."""
    items = []
    kwargs = {
        "KeyConditionExpression": Key("source_id").eq(SOURCE_ID),
        "ScanIndexForward": True,
    }

    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    if not items:
        return pd.DataFrame()

    df = pd.DataFrame(items)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["temp_gap_f"] = df["temp_gap_f"].astype(float)
    df["wind_speed_10m"] = df["wind_speed_10m"].astype(float)
    return df.sort_values("timestamp").reset_index(drop=True)


def generate_plot(df: pd.DataFrame) -> io.BytesIO | None:
    """Plot temperature gap and wind speed over time."""
    if df.empty or len(df) < 2:
        log.info("Not enough history to plot yet (%d point(s))", len(df))
        return None

    sns.set_theme(style="darkgrid", context="talk", font_scale=0.9)

    fig, ax1 = plt.subplots(figsize=(14, 6))
    ax2 = ax1.twinx()

    sns.lineplot(
        data=df,
        x="timestamp",
        y="temp_gap_f",
        ax=ax1,
        color="#D55E00",
        linewidth=3,
        marker="o",
        markersize=8,
        zorder=3,
        label="Temp - Gap b/w Actual & Feels like (F)",
    )
    sns.lineplot(
        data=df,
        x="timestamp",
        y="wind_speed_10m",
        ax=ax2,
        color="#0072B2",
        linewidth=2.5,
        marker="o",
        markersize=7,
        zorder=2,
        label="Wind Speed 10m (km/h)",
    )

    temp_min = df["temp_gap_f"].min()
    temp_max = df["temp_gap_f"].max()
    pad = max(0.2, (temp_max - temp_min) * 0.2)
    ax1.set_ylim(temp_min - pad, temp_max + pad)

    ax1.set_title(
        "Sullivan's Island, Charleston Weather Conditions\n"
        f"Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        fontsize=14,
        fontweight="bold",
        pad=14,
    )
    ax1.set_xlabel("Time (UTC)", labelpad=8)
    ax1.set_ylabel("Temperature Gap (F)", labelpad=8, color="#D55E00")
    ax2.set_ylabel("Wind Speed 10m (km/h)", labelpad=8, color="#0072B2")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    sns.despine(ax=ax1, top=True, right=False)
    sns.despine(ax=ax2, top=True, left=False)
    fig.autofmt_xdate(rotation=25, ha="right")
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    log.info("Plot generated (%d bytes, %d points)", len(buf.getvalue()), len(df))
    return buf


def push_plot(buf: io.BytesIO) -> None:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=PLOT_KEY,
        Body=buf.getvalue(),
        ContentType="image/png",
    )
    log.info("Uploaded %s to s3://%s", PLOT_KEY, S3_BUCKET)


def main():
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.Table(TABLE_NAME)

    entry = fetch_weather()
    table.put_item(Item=entry)

    log.info(
        "WEATHER | temp=%.1f F | feels_like=%.1f F | gap=%.1f F | wind=%.1f km/h | precip=%.1f mm",
        entry["temperature_2m"],
        entry["apparent_temperature"],
        entry["temp_gap_f"],
        entry["wind_speed_10m"],
        entry["precipitation"],
    )

    history = fetch_history(table)
    plot_buf = generate_plot(history)
    if plot_buf:
        push_plot(plot_buf)


if __name__ == "__main__":
    main()
