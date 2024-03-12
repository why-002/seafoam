import time, random, string
from locust import HttpUser, task, between, constant_throughput, FastHttpUser

class QuickstartUser(FastHttpUser):
    wait_time = constant_throughput(1000)
    current_keys = list()
    concurrency = 100

    def on_start(self):
        self.client.post("/set", json={"key": "bar", "value": "baz", "msg_id" : 0})
        self.current_keys.append("bar")
    @task(9)
    def get(self):
        if len(self.current_keys) != 0:
            self.client.post("/get", json={"key": random.choice(self.current_keys), "msg_id" : 0})

    @task
    def set(self):
        if random.randint(0,10) % 10 == 0 or len(self.current_keys) == 0 or len(self.current_keys) > 10000:
            new = random.choice(string.ascii_lowercase) + random.choice(string.ascii_lowercase) + random.choice(string.ascii_lowercase) + random.choice(string.ascii_lowercase)
            self.client.post("/set", json={"key": new, "value": "baz", "msg_id" : 0})
            self.current_keys.append(new)
        else:
            key = random.choice(self.current_keys)
            self.client.post("/set", json={"key": key, "value": "baz", "msg_id" : 0})