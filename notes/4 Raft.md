## 0. Brief

<img src = "picture\1.png" width = 500>

Replicated state machines are typically implemented using a replicated log, as shown in Figure above. Each server stores a log containing a series of commands, which its state machine executes in order. Each log contains the same commands in the same order, so each state machine processes the same sequence of commands. 

Raft is an algorithm for managing a replicated log of the form described above. Raft implements consensus by first electing a distinguished *leader*, and giving the leader complete responsibility for managing the replicated log. The leader accepts log entries from clients, replicates them on other servers, and tells server when it is safe to apply log entries to their state machine. 

Given the leader approach, Raft decomposes the consensus problem into three relatively independently subproblems:

- **Leader election**: A new leader must be chosen when an existing leader fails.
- **Log replication**: the leader must accept log entries from clients and replicate them across the cluster. 
- **Safety**: Raft must satisfy the State Machine Safety Property. 

## 1. Raft Basics

A Raft cluster typically contains five servers, which allows the system to tolerate two failures. At any given time each server is in one of three states: leader, follower or candidate.

<img src = "picture\2.png" width = 500>

Follower are passive. They issues no requests on their own but simply responses to the leader and candidates. The leader handles all client requests. The third state, candidate, is used to elect a new leader. 

Raft divides time into *terms* of arbitrary length. Each term begins with an election, where one or more candidates attempt to become leader. If a candidate wins the election, then it serves as leader for the rest of the term. 

Terms act as a logical lock in Raft, and they allow servers to detect obsolete information such as stale leaders. Each server stores a current term number, which increases monotonically over time. 

