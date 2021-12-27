namespace FSharp.MessageDb.Producers

open System.Threading.Tasks
open FsToolkit.ErrorHandling
open FSharp.MessageDb

module EventStore =

    type Codec<'TEvent, 'TError> =
        { serialize: 'TEvent -> UnrecordedMessage
          deserialize: RecordedMessage -> 'TEvent }

    type EventStore<'TEvent> =
        { appendEvents: string
            -> ExpectedVersion
            -> 'TEvent list
            -> Task<Result<StreamVersion, WrongExpectedVersion * 'TEvent list>>
          loadEvents: string -> Task<StreamVersion * 'TEvent list> }

    let private appendEvents (client: StatelessClient) (codec: Codec<'TEvent, 'TError>) =
        fun streamName expectedVersion (events: 'TEvent list) ->
            client.WriteMessages(streamName, (List.map codec.serialize events), expectedVersion)
            |> TaskResult.bindError (fun (e, catchupEvents) ->
                List.map codec.deserialize catchupEvents
                |> fun r -> TaskResult.error (e, r))

    let private loadEvents (client: StatelessClient) (codec: Codec<'TEvent, 'TError>) =
        fun streamName ->
            client.GetStreamMessages(streamName)
            |> Task.map (fun msgs ->
                List.tryLast msgs
                |> Option.map (fun msg -> msg.version)
                |> Option.defaultValue NoStream,
                List.map codec.deserialize msgs)

    let start (client: StatelessClient) (codec: Codec<'TEvent, 'TError>) : EventStore<'TEvent> =
        { appendEvents = appendEvents client codec
          loadEvents = loadEvents client codec }

/// Loosely based on https://thinkbeforecoding.com/post/2021/12/17/functional-event-sourcing-decider
module EventSourcedProducer =

    type Decider<'TCommand, 'TEvent, 'TState, 'TError> =
        { decide: 'TCommand -> 'TState -> Result<'TEvent list, 'TError>
          evolve: 'TState -> 'TEvent -> 'TState
          initialState: 'TState
          isTerminal: 'TState -> bool }

    module WithEventStore =
        let start
            (eventStore: EventStore.EventStore<'TEvent>)
            (decider: Decider<'TCommand, 'TEvent, 'TState, 'TError>)
            : string -> 'TCommand -> Task<Result<'TEvent list, 'TError>> =
            fun (streamName: string) (command: 'TCommand) ->
                let rec handle (version: ExpectedVersion) state =
                    taskResult {
                        // get events from decision
                        let! events = decider.decide command state

                        return!
                            eventStore.appendEvents streamName version events
                            |> Task.bind (function
                                | Ok _globalPosition ->
                                    // save succeeded, we can return events
                                    TaskResult.ok events
                                | Error (WrongExpectedVersion actualVersion, catchupEvents) ->
                                    // it failed, but we now have the events that we missed
                                    // catch up the current state to the actual state in the database
                                    let actualState = List.fold decider.evolve state catchupEvents
                                    // and try again
                                    handle (ExpectedVersion.ofStreamVersion actualVersion) actualState)
                    }
                taskResult {
                    // load past events
                    let! version, pastEvents = eventStore.loadEvents streamName

                    // compute current state
                    let state = List.fold decider.evolve decider.initialState pastEvents
                    return! handle (ExpectedVersion.ofStreamVersion version) state
                }
