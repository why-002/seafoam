import time
from locust import HttpUser, task, between, constant_throughput

class QuickstartUser(HttpUser):
    wait_time = constant_throughput(100)
    @task
    def hello_world(self):
        self.client.get("/")
        self.client.post("/echo", "foo")

    @task
    def view_items(self):
        self.client.post("/echo/uppercase", "bar")
    
    @task(8)
    def view_json(self):
        self.client.post("/echo/json", json={"type": "echo", "echo": "hello", "msg_id" : 0})