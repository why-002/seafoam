import time
from locust import HttpUser, task, between, constant_throughput, FastHttpUser

class QuickstartUser(FastHttpUser):
    wait_time = constant_throughput(1000)

    @task(9)
    def get(self):
        self.client.post("/get", json={"key": "foo", "msg_id" : 0})

    @task
    def set(self):
        self.client.post("/set", json={"key": "foo", "value": "baz", "msg_id" : 0})