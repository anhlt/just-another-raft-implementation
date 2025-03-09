# Node state

The state of a Raft node is defined by the NodeState class. It can be one of the following:

- Follower
- Candidate
- Leader


# Node properties
A Raft node has the following properties:

- currentTerm: The latest term the node has seen. Increase every time the election happen
- votedFor: The candidate ID the node voted for in the current term.
- log: The log entries the node has recorded.
- commitLength: The index of the highest log entry known to be applied or committed.

These properties should be persisted to disk so the node can recover its state after a crash. Whenever the node updates any of these properties, they must be stored on disk before sending messages to other nodes

```scala mdoc

import com.grok.raft.core.internal.Candidate
import com.grok.raft.core.internal.NodeAddress
import com.grok.raft.core.internal.LogEntry


val candidateNode = Candidate(
	address=NodeAddress("192.168.0.1", 8000),
	currentTerm=1L,
	votedFor=None,
	log=List.empty[LogEntry],
	commitLength=0L,
)

println(candidateNode)
```


