# Candidate State

## Election Time

When the node suspects the leader has failed, or on election timeout it doesn't gather enough votes, the node will enter the Candidate state, nominate itself as Leader, and send VoteRequest to other nodes.

In detail, the following actions will be taken:
- Increase CurrentTerm by one. Every Leader has its own term, and in case a new election happens, the term needs to be increased.
- Update VoteFor to the current NodeAddress.
- Add own NodeAddress to the VoteReceived set.
- If the length of the log is larger than 0, set LastTerm to the term of the last element in the log.


## On VoteRequest

When a node receives a VoteRequest, it will check if the request is valid, and if so, it will vote for the candidate. There are 2 conditions for accepting a VoteRequest: 


```scala
val VoteRequest(proposedLeaderAddress ,candidateTerm, candidateLastLogIndex, candidateLastLogTerm) = voteRequest

case class LogState(lastLogIndex: Long, lastLogTerm: Option[Long], lastAppliedIndex: Long = 0)
```

1. Check id term is ok. The candidate term should be larger than current term, or in case of equal terms, the node has not voted for another candidate in the current term. Because in one term there could be many retry elections, and the node should vote only once. 


```scala

(candidateTerm  > currentTerm) || ((candidateTerm == currentTerm) && votedFor.contains(proposedLeaderAddress))

2. Check if the log is up to date. Ensure the candidate has the most recent log.

```scala

candidateLastLogTerm > logState.lastLogTerm.getOrElse(0L) || (candidateLastLogTerm == logState.lastLogTerm.getOrElse(0L) && candidateLastLogIndex >= logState.lastLogIndex)
```
