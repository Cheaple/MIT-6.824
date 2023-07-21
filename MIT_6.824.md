# MIT 6.824

## Lecture 4  Fault-Tolerant (FT) Virtual Machine

Two types of replications:

+ State transfer (replicate the whole state)
+ ***Replicated state machine*** (replicate the new input which leads to state transfer)
  + More efficient but less robost
  + A **virtual machine** (VM) running on top of a hypervisor is an excellent platform for implementing the state-machine approach. A VM can be considered a well-defined state machine. A FT VM is an OS-level replication, rather than application level.

Since most servers or services have some operations (such as reading the clock cycle counter of the processor and IO completion interrupts) that are not **deterministic**, extra coordination must be used to ensure that a primary and backup are kept in sync.

The production replaying supports **only uni-processor**. Multi-processor is not supported, since nearly every access to shared memory can be a non-deterministic operation.

#### 2.  Basic FT Design

![image-20230630100349141](images\image-20230630100945422.png)

Shared vs. Non-shared Disk: shared disk is less complicated, but the primary and backup cannot be too far apart.

##### 2.1  Deterministic Replay Implementation

If two deterministic state machines are started in the same initial state and provided the exact same inputs in the same order, then they will go through the same sequences of states and produce the same outputs. 

VMware ***deterministic replay*** records the inputs of a VM and all possible non-determinism associated with the VM execution in a stream of **log entries** written to a log file. The VM execution may be exactly replayed later by reading the log entries from the file.

##### 2.2  FT Protocol

Instead of writing the log entries to disk, we send them to the backup VM via the ***logging channel***.

**Output Requirement**: if the backup VM ever takes over the primary, the backup VM will continue executing in a way that is entirely consistent with all outputs that the primary VM has sent to the external world.

**Output Rule**: the primary VM may not send an output to the external world, until the backup VM has received and *acknowledged* the log entry associated with the operation producing the output.

##### 2.3  Detecting and Responding to Failure

A failure is declared if heartbeating or logging traffic has stopped for longer than a specific timeout.

The time for the backup to *go live* is roughly equal to the failure detection time plus the current execution lag time. If the backup VM continues to lag behind, we continue to gradually reduce the primary VM's CPU limit.

## Lecture 6 & 7  Raft

*Raft*: a consensus algorithm for managing a replicated log.

![image-20230709162439930](images\image-20230709162439930.png)

![image-20230710011827317](images\image-20230710011827317.png)

##### 5.2 Leader Election

length of election timer:

+ minimum: heartbeat interval
+ maximum: determined by the upper bound of recovery time

##### 5.3 Log Replication

A log entry is **committed** once the leader that created the entry has replicated it on a **majority** of the servers. The leader keeps track of the highest index it knows to be committed, and it includes that index in future AppendEntries RPCs (including heartbeats) so that the other servers eventually find out. Once a follower learns that a log entry is committed, it applies the entry to its local state machine (in log order).

#### Lab 2  Implementation

Some irratating bugs in my implementations:

+ Each time append, commit, or apply a new entry, broadcast it immediately.
+ If using *channel* to implement asynchronous applier, pay attention to avoiding deadlocks!
+ Before sending out a AppendEntries RPC, make sure that the sender is still a LEADER!

## Lecture 8  ZooKeeper

***Linearizability***: The execution history is linearizable if there exists an total order of operations that matches real-time for non-concurrent requests and each read sees the most recent write in the order.

Why ZooKeeper?

+ API for general purpose
+ achieve n-time performance with n-time CPU

ZK Guarantees:

+ **Linearizable Write**s (not read): all requests that update the state of ZooKeeper are serializable and respect precedence.
+ **FIFO client Order**: all requests from a given client are executed in the order that they were sent by the client.

Because only update requests are linearizable, ZooKeeper processes read requests locally at each replica. This allows the service to scale linearly as servers are added to the system. ZooKeeper implements **watches** to allow clients to receive timely notifications of changes without requiring polling.

## Lecture 9  CRAQ

*CRAQ*: Chain Replication with Apportioned Queries

![image-20230716155541075](images\image-20230716155541075.png)

When a node is down, its successor and predecessor will connect each other to remove the down node. However, in the down node's perspective, all other nodes get down, so it will try to transform itself both HEAD and TAIL. This leads to **split brain** problem, which bring the need for an externel manager (maybe running on Raft, which is fault-torlerent).

## Lecture 10  Cloud Replicated DB, Aurora

