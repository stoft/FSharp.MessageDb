module Producer.Tests

open Expecto
open Expecto.Flip
open FSharp.MessageDb
open FsToolkit.ErrorHandling
open Serilog

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
      evolve = fun state event -> state
      initialState = []
      isTerminal = fun _ -> false }

let codec: Codec<string, string> =
    { serialize =
        fun event ->
            { eventType = "test"
              data = "{}"
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
                    let handler = Producers.EventSourcedProducer.WithEventStore.start eventStore decider

                    handler "test-ESProducer" ""
                    |> TaskResult.bind (fun _ ->

                        store.GetStreamMessages("test-ESProducer")
                        |> TaskResult.ofTask)
                    |> TaskResult.map (fun (result :: _) ->
                        Expect.equal "" "" result.data
                        [ result ])

                } ] ]
