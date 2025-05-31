from datetime import timedelta, datetime
from prefect import flow, task
from prefect.schedules import Interval

@task
def say_hello(datetime: datetime = datetime.now()):
    print(f"👋 Hello on schedule: {datetime.strftime('%Y-%m-%d %H:%M:%S')}!")

@flow
def scheduled_flow():
    say_hello()

if __name__ == "__main__":
    schedule = Interval(
        timedelta(minutes=1),
        anchor_date=datetime.now(),
        timezone="America/Chicago"
        )
    scheduled_flow.serve(
        name="hourly-example",
        schedule=schedule)