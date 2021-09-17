# Mit6.824 LAB

This repo is for implementation of all labs in mit6.824

## Lab3 Log

Design:

Write/Read from leader, and Get() is also add to raft log in case of read minority stale data when network partition happend.

- Serializability: Since read write from the leader, serializability is guaranteed
- Availability: Fault tolerance is implemented by raft election mechanism
- Consistancy: final consistancy

### Sep 6

finished first two test of lab3A

### Sep 7

finished the third test of lab3A

### Sep 8

Add deduplication to apply cmd at server

### Sep 9

Read operation is not idempotent, and idempotent operation should not return error. stuck at test(partitions, one client (3A))

### Sep 13

test(partitions, one client (3A)) passed by handling the duplicate call when origin one is still in processing

### Sep 14

fix bug in raft::checkApply(), multi-thread apply should avoid duplicate apply as much as possible
restart test failed due to not linearizable 

### Sep 15

fixed restart issue: subscribeMap `map[serialID]chan int -> map[clientID]map[serialID]chan int`
Pass all test occasionally.

Bug found:

figure8 + clerk idempotent makes the clerk retry forever and the out-dated entry can not commit due to lower term. And new opts from current
term can't be sent to raft due to idempotent.

How to fix:

Only check idempotent req for same term, which means when term changes we should clear the subMap

### Sep 16

3A All test pass.

Test: one client (3A) ...

  ... Passed --  15.1  5  7690 1461

Test: ops complete fast enough (3A) ...

Test: many clients (3A) ...

  ... Passed --  15.6  5  8858 3431

Test: unreliable net, many clients (3A) ...

  ... Passed --  29.6  5  2748  208

Test: concurrent append to same key, unreliable (3A) ...

  ... Passed --   5.0  3   280   52

Test: progress in majority (3A) ...

  ... Passed --   1.0  5   301    2

Test: no progress in minority (3A) ...

  ... Passed --   1.0  5  1013    3

Test: completion after heal (3A) ...

  ... Passed --   1.1  5    93    3

Test: partitions, one client (3A) ...

  ... Passed --  22.8  5 22313  949

Test: partitions, many clients (3A) ...

  ... Passed --  23.7  5 26311 2962

Test: restarts, one client (3A) ...

  ... Passed --  21.8  5  7368 1386

Test: restarts, many clients (3A) ...

  ... Passed --  21.7  5  9011 3541

Test: unreliable net, restarts, many clients (3A) ...

  ... Passed --  34.2  5  3000  244

Test: restarts, partitions, many clients (3A) ...

  ... Passed --  27.3  5 27763 3067

Test: unreliable net, restarts, partitions, many clients (3A) ...

  ... Passed --  44.8  5  9940  156

Test: unreliable net, restarts, partitions, random keys, many clients (3A) ...

  ... Passed --  55.4  7 11167  438

PASS

ok      6.824/kvraft    331.020s

### Sep 17

Refactory code and finish lab3B

Test: InstallSnapshot RPC (3B) ...

  ... Passed --   3.1  3  2213   63

Test: snapshot size is reasonable (3B) ...

  ... Passed --   8.1  3  2488  800

Test: ops complete fast enough (3B) ...

Test: restarts, snapshots, one client (3B) ...

  ... Passed --  21.7  5  7671 1440

Test: restarts, snapshots, many clients (3B) ...

  ... Passed --  22.1  5 13604 6571

Test: unreliable net, snapshots, many clients (3B) ...

  ... Passed --  28.4  5  2844  234

Test: unreliable net, restarts, snapshots, many clients (3B) ...

  ... Passed --  35.1  5  2533  156

Test: unreliable net, restarts, partitions, snapshots, many clients (3B) ...

  ... Passed --  42.9  5  7540  177

Test: unreliable net, restarts, partitions, snapshots, random keys, many clients (3B) ...

  ... Passed --  51.7  7 15370  447

PASS

ok      6.824/kvraft    224.004s

## Todo

1. raft: batch process start cmd instead of one cmd one goroutine, checkApply
currently use heartbead interval 120ms which is too high, should be less than 30ms

Tongkai Zhang
