module Producer.Tests

open Expecto
open Expecto.Flip
open FSharp.MessageDb
open FsToolkit.ErrorHandling
open FSharp.MessageDb.Producers.EventSourcedProducer
open FSharp.MessageDb.Producers.EventStore

let cnxString =
    DbConnectionString.create
        { dbHost = "localhost"
          dbUsername = "admin"
          dbPassword = "secret"
          dbPort = 11111
          dbName = "message_store"
          dbSchema = "message_store" }
    |> DbConnectionString.toString

let store = StatelessClient(cnxString)

let writeMsg streamName guid =
    let input =
        { id = guid
          eventType = "test-event"
          metadata = None
          data = "{}" }

    store.WriteMessage(streamName, input).Result

let teardown id = store.DeleteMessage(id)

let decider: Decider<string, string, _, string> =
    { decide = fun cmd state -> Ok [ cmd ]
      evolve = fun state event -> event :: state
      initialState = []
      isTerminal = fun _ -> false }

let codec: Codec<string, string> =
    { serialize =
        fun event ->
            { eventType = "test"
              data = $"\"%s{event}\""
              metadata = None
              id = System.Guid.NewGuid() }
      deserialize = fun (msg: RecordedMessage) -> msg.data }

let eventStore = Producers.EventStore.start store codec

[<Tests>]
let tests =
    testList
        "EventSourcingProducer"
        [ testList
              "decider"
              [ test "should succeed" {
                    let handler = Producers.EventSourcedProducer.start eventStore decider

                    let t =
                        handler "test-ESProducer" ""
                        |> TaskResult.bind (fun _ ->
                            store.GetStreamMessages("test-ESProducer")
                            |> TaskResult.ofTask)
                        |> TaskResult.map (fun (result :: _) -> result)
                    let result = t.Result
                    Expect.wantOk "" result
                    |> fun r ->
                        teardown r.id |> ignore
                        Expect.equal "" r.data "{}"
                } ] ]
