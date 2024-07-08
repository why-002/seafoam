# Seafoam
A lightweight kv store using a Raft-based protocol for replication and lockless reads to allow for maximum read performance.

## Inspiration
When I was reading Roberto Vitillo's "Understanding Distributed Systems" I came across a section on the Raft protocol and decided that I wanted to do a project that involved it. I was also enrolled in CSCI 5105: Understanding Distributed Systems at the University of Minnesota at the time, so I took some inspiration from a class project and extended the idea in an implementation in a different language.

I also have taken a handful of classes focusing on computer architecture, so I wanted to see how much optimization I could do in a lower-level language to boost my system's performance.

---

## What it does
1. It has a few main API endpoints including "/get" and "/set", which accept POST requests for the storage of json data.

2. It then forwards the updates to the leading node where they are then replicated to every node.

3. Just like that, all nodes have the updates and respond to any user read requests!

---

## How it's built
This project is implemented in Rust using the Tokio runtime for async capabillities. Since I was looking to minimize unnecessary overhead, I chose to use Hyper for handling http requests since many other rust frameworks for handling http requests wrap Hyper anyways.

My storage primitive of choice for this project was flashmap by Cassie343. I chose flashmap because it has lockless reads, and since my goal was to maximize my read throughput, this allowed me to scale the reads effictively linearly with cores.

---

## Challenges I ran into
Getting the Raft protocol to run properly was a bit of a challenge in the beginning. This is primarily because debugging distributed systems is in and of itself a complex process due to the many possible irregularities that can occur.

## Accomplishments that I'm proud of / What I Learned
- Many hours reading and learning how the Raft protocol works.
- Building my first project in Rust.
- First major experience working in a multi-threaded asynchrous enviroment.

## What's next for Seafoam
- Backing up logs to disk
- Pipelined Writes
- Group membership changes
- Easy delpoyment with Docker

---

## Setup and Execution
### Run single instance on port 3000:
```
$ cd seafoam

$ cargo r --release --bin seafoam-server 3000 3010 // run single instance on port 3000
```

### Run three instances on localhost:
```
$ cd seafoam

$ cargo r --release --bin seafoam-server 3000 3010 127.0.0.1:5010 127.0.0.1:6010
```
```
$ cd seafoam

$ cargo r --release --bin seafoam-server 5000 5010 127.0.0.1:5010 127.0.0.1:6010
```
```
$ cd seafoam

$ cargo r --release --bin seafoam-server 6000 6010 127.0.0.1:5010 127.0.0.1:6010
```

## API usage
The api currently has 4 gRPC endpoints:
1. Get
```javascript
{
  "key": "foo",
  "msg_id": 0
}
```
2. Set
```javascript
{
  "key": "foo",
  "value": {
    "id": 0
    "name": "bar"
  },
  "msg_id": 0
}
```
3. Cas
```javascript
{
  "key": "foo",
  "old_value": "bar",
  "new_value": {
    "id": 0
    "name": "bar"
  },
  "msg_id": 0
}
```
4. Delete
```javascript
{
  "key": "foo",
  "msg_id": 0,
  "error_if_not_key": false
}
```
