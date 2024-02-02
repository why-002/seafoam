import time, random, string
from locust import HttpUser, task, between, constant_throughput, FastHttpUser, gevent

class QuickstartUser(FastHttpUser):
    wait_time = constant_throughput(1000)
    current_keys = list()
    @task(9)
    def get(self):
        def multipleGetRequests():
            if len(self.current_keys) == 0:
                self.client.post("/get", json={"key": "foo", "msg_id" : 0})
            else:
                self.client.post("/get", json={"key": random.choice(self.current_keys), "msg_id" : 0})
        pool = gevent.pool.Pool(20)
        for i in range(0,20):
            pool.spawn(multipleGetRequests)
        pool.join()


    @task
    def set(self):
        if random.randint(0,10) % 10 == 0 or len(self.current_keys) == 0:
            new = random.choice(string.ascii_lowercase) + random.choice(string.ascii_lowercase) + random.choice(string.ascii_lowercase) + random.choice(string.ascii_lowercase)
            self.current_keys.append(new)
        else:
            key = random.choice(self.current_keys)
            self.client.post("/set", json={"key": key, "value": "baz", "msg_id" : 0})