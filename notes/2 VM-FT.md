## 1. Introduction

A common approach to implementing fault-tolerant servers is the primary/backup approach, where a backup server is always available to take over when the primary server fails.

Replication is good for *fail-stop* failure of a single replica. When a replica fails, it merely ceases executing and does not cause incorrect externally visible actions. 

## 2. Basic FT Design

For a given VM for which we desire to provide fault tolerance (the *primary* VM), we run a *backup* VM on a different physical server that is kept in sync and executes identically to the primary virtual machine, with a small time lag.

The virtual disk for the VMs are on shared storage. Only primary VM advertises its presence on the network, so all network inputs come to the primary VM.

All input that the primary VM receives is sent to the backup VM via a network connection called the *logging channel*. Additional information is transmitted to ensure that backup VM runs non-deterministic operations in the same way as the primary.

**2.1 Deterministic Replay Implementation**

Deterministic replay records the inputs of a VM and all possible non-determinism associated with the VM execution in a stream of log entries written into a log file.

Only non-deterministic operations needs to be sent, since deterministic operations must derive the identical results. 

Each log entry consists of instruction #, type and data. Let's take FT's handling of timer interrupts as an example. 

Primary:

1. FT fields the timer interrupt.
2. FT reads the instruction # from CPU.
3. FT sends "timer interrupt at instruction # X" on the logging channel.
4. FT delivers interrupt to primary and resumes it.

Backup:

1. ignores its own timer hardware.
2. FT sees log entry **before** backup gets to instruction # X.
3. FT tells the CPU to transfer control to FT at instruction # X.
4. FT mimics a timer interrupt that backup guest sees.

**2.2 FT Protocol**

**Output Requirement**: if the backup VM ever takes over after a failure of the primary, the backup VM will continue executing in a way that is entirely consistent with all outputs that the primary VM has sent to the external world. 

The output requirement can be achieved by delaying any external output (typically a network packet) until the backup VM has received all information that will allow it to replay execution at least to the point of that output operation.

**Output Rule**: the primary VM may not send an output to the external world, until the backup has received and acknowledged the log entry associated with the operation producing the output.

Note that the output rule does not say anything about stopping the execution of the primary VM. We need only delay the sending of the output, but the VM itself can continue executing. 

**2.3 Detecting and Responding to Failure**

If the primary VM fails, the backup VM should *go live*. Because it lag in execution, the backup VM will likely have a number of log entries that it has received and acknowledged, but have not yet been consumed. The backup VM must continue replaying its execution from the log entries until it have consumed the last log entry.

For the purposes of networking, VMware FT automatically advertises the MAC address of the new primary VM on the network, so that physical network switches will know on what server the new primary VM is located. 

VMware FT uses UDP heartbeating between servers that are running fault-tolerant VMs to detect when a server may have crashed. 

If the backup VM then goes live while the primary VM is still running, there will be data corruption and other problems. This is called the split-brain problem. Hence, we must ensure that only one of the primary or backup VM goes live when a failure is detected. 

When either a primary or backup VM wants to go live, it executes an atomic test-and-set operation on the shared storage. If the operation succeeds, the VM is allowed to go live. If the operation fails, then the other VM must have already gone live, so the current VM actually halts itself.

