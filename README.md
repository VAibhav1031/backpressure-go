## Backpressure-Go

This small Project is the demonstration of the `backpressure` feature used in the backend system for the high inflow of the traffic, so that they can control things out without getting  service down .

### Problem to solve:
The Problem to Simulate: Imagine a fast client sending 10,000 HTTP requests per second to your backend, but your database can only handle 500 writes per second. If you just accept all requests into memory, your server will hit an Out-Of-Memory (OOM) panic and crash.


### Solution:
- Create a bounded channel or queue (eg. 5000 burst/queue_length) in Golang.

- If the queue fills up, implement Load Shedding: instantly return HTTP 429 Too Many Requests or HTTP 503 Service Unavailable instead of queuing forever.

- Implement a Leaky Bucket or Token Bucket algorithm to smoothly drain items from the queue into your DB.


### What i did ::

I Implemented as the solution with few golang quirks to make it work efficiently by creating the leaky bucket Algorithm which have burst time and eviction rate/second , based on that we will use the channel for the queue and based on the timer and channel batched-length.


HTTP handler --> Handling incoming http requests and sending to channel , if the incoming request is more than burst it will give `too-many-requests`(429) error else it will give 202 Accepted Response for sucess.
FLow Manager --> leaky bucket algo. implementation and based onthat  moving the request forward using other channels 
Batch Manager --> this use batch length and timer technique to give the  all request to the db {this help in increasing throughput because of sending one request at a time to the db worker will waste whole db flow and reduce the response time.}
DB-Worker --> DB workers acts on the given batch and give to the db commit handler 
