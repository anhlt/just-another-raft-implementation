# AGENTS.md

## Build/Test Commands
- **Build**: `sbt compile`
- **Test all**: `sbt test`
- **Test single file**: `sbt "testOnly com.grok.raft.core.internal.NodeSuite"`
- **Format code**: `sbt scalafmt`
- **Format check**: `sbt scalafmtCheck`

## Code Style & Conventions
- **Scala version**: 3.7.3 with tpolecat compiler flags
- **Max line length**: 120 characters
- **Formatting**: Use scalafmt with `align.preset = more`
- **Imports**: Group external imports first, then internal `com.grok.raft` imports
- **Package structure**: `com.grok.raft.{core|effects}`
- **Error handling**: Use custom error types (`RaftError`, `LogError`, `MembershipError`) extending `BaseError`
- **Effects**: Cats Effect 3 with `F[_]: MonadThrow` constraints
- **Logging**: Use `org.typelevel.log4cats` with `trace`, `debug`, `info` syntax
- **Testing**: MUnit with `munit-cats-effect` for effect testing
- **Types**: Use explicit types for public APIs, infer for internal/private methods
- **Pattern matching**: Use match expressions with exhaustive patterns
- **Naming**: camelCase for methods/variables, PascalCase for classes/traits/objects
- **Comments**: Scaladoc for public APIs, minimal inline comments
- **State**: Immutable data structures with case classes, functional state updates