A history describes a time-line of possibly concurrent operations. Each operation has client start and finish times. 

>C1: |-Wx1-| |-Wx2-|
>
>C2:     |---Rx2---|
>
>|- indicates the time at which client sent request.
>
>-| indicates the time at which the client received the reply.
>
>Wx1 means put(x, 1).
>
>Rx2 means get(x) -> 2.

A history is linearizable if you can find a point in time for each operation between its start and finish such that the history's result values are the same as serial execution in point order.

>C1: |--Wx1--| |--Wx2--|
>C2:      |----Rx2----|
>C3:         |--Rx1--|
>
>This order of points satisfies the rules: Wx1 Rx1 Wx2 Rx2

Linearizability often allows multiple different outcomes. Instead of predicting in advance, we can check afterwards. 

The service probably didn't execute the operations at those points. But We don't care how the service operated internally. We only concern that the client-visible results could have resulted from execution in some point order.

If we want linearizability: 

- Once any read sees a write, all strictly-subsequent reads must also see it. This rules out split-brain problem.
- Can't forget a revealed write. This rules out forgetting data due to a crash or reading from stale caches. 

There are other consistency models like eventual consistency. In this case, a read consults any replica. A write updates any replica. Replicas synchronize updates in the background so eventually, other replicas will see my updates.

But eventual consistency exposes some anomalies to application programmer:

- A read may not see the most recent write.
- Writes may appear out of order.
- Different clients may see different data.
- Concurrent writes to same item need to be resolved somehow.

