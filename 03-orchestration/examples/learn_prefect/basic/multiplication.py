from prefect import flow, task

@task
def add(x, y):
    return x + y

@task
def multiply(z):
    return z * 10

@flow
def math_flow(a: int, b: int):
    sum_result = add(a, b)
    product = multiply(sum_result)
    print("Final result:", product)

if __name__ == "__main__":
    math_flow(3, 5)
