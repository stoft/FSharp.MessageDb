# FSharp.MessageDb

A simple F# wrapper around [message-db](https://github.com/message-db/message-db)

**Current status**: alpha, WIP, possibly broken, definitely not in production.

Due to family life this will most likely not change. Feel free to PR or fork it!

Parts of it are loosely based on https://thinkbeforecoding.com/post/2021/12/17/functional-event-sourcing-decider

A proper [Equinox](https://github.com/jet/equinox) adapter should probably be written.

## Contents

- SqlLib: low-level wrapper in Npgsql.FSharp around the message-db functions/stored procedures that will allow you to bring your own connection, transaction and async implementation in whatever way you like
- SqlClient: mid-level wrapper that wraps the above in .NET Tasks, you can still bring your own connection
- StatelessClient/StatefulClient: FauxO wrappers that will either create new connections (Stateless) on each call or reuse a provided connection (Stateful)
- EventSourcingProducer: a producer that follows the ES decider pattern with optimistic concurrency control
- ConsumerLib: good to have consumer functions
- StatelessConsumer/Stateful Category Consumer: a consumer that allows either an exclusive or competing consumer pattern using PGSQL advisory locks on a category. The stateful consumer will save state in the message-db (every 20 messages), whereas the stateless consumer leaves this up to the handler function to manage. Both batch read 20 messages (currently fixed) at a time.
