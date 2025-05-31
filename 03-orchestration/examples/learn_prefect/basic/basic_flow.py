from prefect import flow, task

@task
def say_hello():
    print("Hello from Prefect!")

@task
def say_goodbye():
    print("Goodbye from Prefect!")

@flow
def my_first_flow():
    say_hello()
    say_goodbye()

if __name__ == "__main__":
    my_first_flow()
