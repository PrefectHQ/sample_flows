import datetime
import pendulum
import requests

from prefect import task, Flow, Parameter, case
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.notifications.slack_task import SlackTask
from prefect.artifacts import create_link
from prefect.storage import Docker
from prefect.run_configs import DockerRun
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

# TASK DEFINITIONS
# Parameter Task takes a value at runtime
city = Parameter(name="City", default="San Francisco")

# PrefectSecret Task stored in Hashicorp Vault, populated at runtime
api_key = PrefectSecret("WEATHER_API_KEY")

# Extraction Task pulls 5-day, 3-hour forcast for the provided City
@task(max_retries=2, retry_delay=datetime.timedelta(seconds=5))
def pull_forecast(city, api_key):
    base_url = "http://api.openweathermap.org/data/2.5/forecast?"
    url = base_url + "appid=" + api_key + "&q=" + city
    link_artifact = create_link(url)
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    return data

# Analysis Task determines whether there will be rain in forecast data
@task(tags=["database", "analysis"])
def is_raining_this_week(data):
    rain = [
        forecast["rain"].get("3h", 0) for forecast in data["list"] if "rain" in forecast
    ]
    return True if sum([s >= 1 for s in rain]) >= 1 else False

# Notification Task sends message to Cloud once authenticated with a webhook
rain_notification, dry_notification = SlackTask(
    message="RAIN in the forecast for this week!",
    webhook_secret="SLACK_WEBHOOK_URL_MHQ",
), SlackTask(
    message="NO RAIN in the forecast for this week!",
    webhook_secret="SLACK_WEBHOOK_URL_MHQ",
)

# FLOW DEFINITION
with Flow(
    "Rain Flow",
    # Storage for code pushed to Docker Registry, image pulled at runtime
    storage=Docker(
        registry_url="kaliserichmond",
        image_name="flows",
        image_tag="weather-flow"
    ),
    # Flow Run configuration for env vars and label matching
    run_config=DockerRun(
        env={"sample_key": "sample_value"},
        labels=["demo-flow"]
    ),
    # Schedule via cron string, can be overwritten in UI
    schedule=Schedule(
        clocks=[CronClock("0 12 * * 1-5",
        start_date=pendulum.now(tz="US/Pacific"))]
    )
) as flow:
    # Set up Task dependencies
    forecast = pull_forecast(city=city, api_key=api_key)
    rain = is_raining_this_week(forecast)
    with case(rain, True):
        rain_notification()
    with case(rain, False):
        dry_notification()

# REGISTRATION TO PREFECT CLOUD
flow.register(project_name="Demos")
