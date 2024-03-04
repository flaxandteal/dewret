from dewret.tasks import task

@task()
def increment(num: int):
    return num + 1
