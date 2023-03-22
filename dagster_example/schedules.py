"""Collection of Cereal schedules"""

from dagster import schedule

from dagster_example.jobs import complex_job


"""# https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules
@schedule(
    cron_schedule="0 9 * * 1-5",
    job=complex_job,
    execution_timezone="Europe/Stockholm",
)
def every_weekday_9am(context):
    
    date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return {"ops": {"download_cereals": {"config": {"date": date}}}}
"""