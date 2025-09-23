# Snapshot Installation Protocol

## Overview

The `onSnapshotInstalled` method is called after a snapshot has been successfully installed by the Log layer. This method allows each node type to respond appropriately to snapshot installation and potentially update their state.

## Node Type Behaviors

### Follower
- **Response**: Returns positive acknowledgment with `ackLogIndex = logState.lastLogIndex`
- **State Change**: Maintains follower state (no change)
- **Rationale**: Snapshot installation is normal for followers as part of log replication

### Candidate
- **Response**: Returns positive acknowledgment with `ackLogIndex = logState.lastLogIndex`
- **State Change**: Steps down to Follower state
- **Rationale**: Snapshot installation implies there's a legitimate leader, so candidate should step down
- **Details**: 
  - Sets `currentLeader = None` (will be updated on next LogRequest from leader)
  - Preserves `votedFor` from candidate state

### Leader
- **Response**: Returns positive acknowledgment with `ackLogIndex = logState.lastLogIndex`
- **State Change**: Maintains leader state (unusual scenario)
- **Rationale**: Leaders shouldn't normally receive snapshots, but we acknowledge for consistency

## Response Format

All node types return `LogRequestResponse` with:
- `nodeId`: The node's address
- `currentTerm`: The node's current term
- `ackLogIndex`: `logState.lastLogIndex` (acknowledging up to snapshot's last index)
- `success`: `true` (assumes snapshot installation succeeded)

## Integration

The snapshot installation flow typically follows this pattern:
1. `Log.installSnapshot(snapshot)` - installs the snapshot at the log level
2. `Node.onSnapshotInstalled(logState, clusterConfig)` - updates node state and generates response
3. Response is sent back to the leader (if applicable)

## Testing

Comprehensive tests are available in `SnapshotInstalledSpec.scala` covering:
- Follower acknowledgment behavior
- Candidate step-down behavior  
- Leader acknowledgment behavior
- Response format validation
- Edge cases (empty log state)