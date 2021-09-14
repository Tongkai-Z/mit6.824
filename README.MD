# Mit6.824 LAB

This repo is for implementation of all labs in mit6.824

## Log

Sep 6

finished first two test of lab3A

Sep 7

finished the third test of lab3A

Sep 8

Add deduplication to apply cmd at server

Sep 9

Read operation is not idempotent, and idempotent operation should not return error. stuck at test(partitions, one client (3A))

Sep 13

test(partitions, one client (3A)) passed by handling the duplicate call when origin one is still in processing

Sep 14

fix bug in raft::checkApply(), multi-thread apply should avoid duplicate apply as much as possible
restart test failed due to not linearizable 

Tongkai Zhang
