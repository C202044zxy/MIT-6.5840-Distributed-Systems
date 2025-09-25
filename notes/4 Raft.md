## 0. Brief

<img src = "\picture\1.png" width = 500>

Replicated state machines are typically implemented using a replicated log, as shown in Figure above. Each server stores a log containing a series of commands, which its state machine executes in order. Each log contains the same commands in the same order, so each state machine processes the same sequence of commands. 

Raft is an algorithm for managing a replicated log of the form described above. Raft implements consensus by first electing a distinguished *leader*, and giving the leader complete responsibility for managing the replicated log. The leader accepts log entries from clients, replicates them on other servers, and tells server when it is safe to apply log entries to their state machine. 

Given the leader approach, Raft 