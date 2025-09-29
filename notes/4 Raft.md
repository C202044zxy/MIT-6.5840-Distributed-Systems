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

Raft divides time into *terms* of arbitrary length. Each term begins with an *election*, where one or more candidates attempt to become leader. If a candidate wins the election, then it serves as leader for the rest of the term. 

Terms act as a logical lock in Raft, and they allow servers to detect obsolete information such as stale leaders. Each server stores a current term number, which increases monotonically over time. 

Raft servers communicate using remote procedure calls(RPCs), and the basic consensus algorithm requires two types of RPCs. They are RequestVote and AppendEntries. Additionally, there is an advanced type of RPC called InstallSnapshot, which is used to send snapshot across servers.  

## 2. Leader Election

<img src = "picture\3.png" width = 500>

Raft uses a heartbeat mechanism to trigger leader election. When servers start up, they begin as follower. A server remains in follower state as long as it receives valid RPCs from a leader or candidate.

If a follower receives no communication of a period of time called *election timeout*, then it assumes there is no viable leader and begins an election. 

To begin an election, a follower increases its current term and becomes candidate. A candidate continues in this state until one of the three things happens:

- it wins the election.
- another server establishes itself as leader.
- a period of time goes by with no winner.

A candidate wins an election if it receives votes from a majority of the servers in the full cluster of the same term. Each server will vote for at most one candidate in a given term, on a first-come-first-served basis. 

While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader. If the leader's term is at least as large as the candidate's current term, then the candidate recognizes the leader and returns to follower state. If the term in the RPC is smaller than the candidate's current term, then the candidate reject the RPC and continues in candidate state. 

If many followers become candidates at the same time, votes could be split so that no candidate obtains a majority. To prevent split votes in the first place, election timeout are chosen randomly from a fixed interval(e.g., 150-300ms).

## 3. Log Replication

<img src = "picture\4.png" width = 500>

The leader appends the command to its log as a new entry, then issues AppendEntries RPCs in parallel to follower to replicate the entry. When the entry is safely replicated, the leader applies the entry to its state machine and returns the result of execution to the client. 

Each log entry stores a state machine command along with the term number when the entry was received by the leader. The leader decides when it is safe to apply a log entry to the state machine. Such an entry is called *committed*. 

A log entry is committed once the leader that created the entry has replicated it on a majority of the servers. The leader keeps track of the the highest index it knows to be committed, and include the index in the future AppendEntries RPCs so that other servers eventually find out. 

Raft maintains Log Matching Property to ensure a high level of coherency between logs on different servers:

- If two entries in different logs have same index and term, they store the same command. 
- If two entries in different logs have same index and term, the logs are identical in all preceding entries. 

The first property follows from the fact that a leader creates at most one entry with a given log index in a given term, and log entries never change their position in the log.

The second property is guaranteed by a simple consistency check performed by AppendEntries. When sending an AppendEntries RPC, the leader includes the index and term of the entry in its log that immediately precedes the new entries. If the follower doesn't find an entry in its log with same index and term, it refuses the new entries. 

If inconsistency occurs, the leader forces the follower's log to duplicate its own. This means the conflicting entries in the follower log will be overwritten with the log from the leader's log. 

To achieve consistency across servers, the leader must find the latest log entry that two logs agree, delete any entries in the follower's log after that point. The leader maintains a *nextIndex* for each follower, which is the index of the next log entry the leader will send to follower. 

When a leader first comes to power, it initializes all nextIndex value to the index just after the last one in its log. If a follower's log is inconsistent with leader's, the consistency check will fail and the leader will decrement nextIndex and retry the AppendEntries RPC.

## 4. Safety

<img src = "picture\5.png" width = 500>

**4.1 Election Restriction**

Raft uses a simple approach where it guarantees that all the committed entries from previous terms are present on each subsequent new leader, without the need to transfer those entries to leader. This means that log entries only flow in one direction, from leaders to followers, and leaders never overwrite existing entries in their logs. 

This approach is adding a restriction in voting process. A candidate must contact a majority of servers in order to be elected, which means that every committed entry must be presented in at least one of those servers. If the candidate's log is at least as up-to-date as any other log in that majority, then it will hold all the committed entries. 

The RequestVote RPC implements this restriction: the RPC includes the information about the candidate's log, and the voter denies its vote if its own log is more up-to-date than that of the candidate. 

Raft determines which of two logs is more up-to-date by comparing the index and term of the last log entries in the logs. If two logs have different last term, then the logs with higher last term is more up-to-date. If the logs end with same term, then whichever log is longer is more up-to-date. 

**4.2 Committing Entries from Previous Term**

If a leader crashes before committing an entry, future leaders will attempt to finish replicating the entry. However, a leader can not immediately conclude that an entry from a previous term is committed once it is stored on the majority of servers. The following figure illustrates a situation where an old entry is stored on the majority of servers, yet can still be overwritten by a future leader.

<img src = "picture\6.png" width = 500>

To eliminate the kind of problems, Raft never commits log entries from previous terms by counting replicas. Only log entries from the leader's current term are committed by counting replicas. Once an entry from the current is committed in this way, then all prior entries are committed indirectly because of the Log Matching Property. 

## 5. Log Compaction

Raft's log grows during normal operation to incorporate more client requests, but in pratical system, it can not grow out of bound. Logs need to be compacted. 

Snapshotting is the simplest approach to compaction. In snapshotting, the entire current state is written to a snapshot on stable storage, then the entire log up to that point is discarded. 

<img src = "picture\7.png" width = 500>

Figure 12 shows the basic idea of snapshotting in Raft. Each server takes snapshots independently, covering just the committed entries in its log. Most of the work consists of the state machine writing its current state to the snapshot. Raft also includes a small amount of metadata in the snapshot: the *last included index* is the index of the last log entry that the snapshot replaces, and the *last included term* of this entry. These are preserved to support the AppendEntries consistency check for the first log entry following the snapshot. 

Although servers normally take snapshots independently, the leader must occasionally send snapshots to the follower that lags behind. This happens when the leader has already discarded the next log entry that needs to be sent to a follower. 

<img src = "picture\8.png" width = 500>

The leader uses a new RPC called InstallSnapshot to send snapshot to followers that are too far behind. If the snapshot contains new information not already in the recipient's log, the follower discards its entire log. If instead the follower receives a snapshot that describes a prefix of its log, the log entries covered by snapshot are deleted but entires following the snapshot are retained. 

There are two more issues that impact snapshotting performance. First, server must decide when to snapshot. If a server snapshots too often, it wastes disk bindwidth. If it snapshots too infrequently, it risks exhausting its storage capacity, and it increases the time to replay the logs during restarts. 

The second performance issue is that writting a snapshot can take a significant amount of time, and we do not want this to delay normal operations. The solution is to use copy-on-write techniques so that new updates can be accepted without impacting the snapshot being written. 
