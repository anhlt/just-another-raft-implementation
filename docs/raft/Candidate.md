# Candidate State

## Election Time

When the node suspects the leader has failed, or on election timeout, the node will enter the Candidate state, nominate itself as Leader, and send VoteRequest to other nodes.

In detail, the following actions will be taken:
- Increase CurrentTerm by one. Every Leader has its own term, and in case a new election happens, the term needs to be increased.
- Update VoteFor to the current NodeAddress.
- Add own NodeAddress to the VoteReceived set.
- If the length of the log is larger than 0, set LastTerm to the term of the last element in the log.
