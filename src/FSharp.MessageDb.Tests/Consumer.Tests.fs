module Consumer.Tests

open Expecto
open Expecto.Flip
open FSharp.MessageDb
open FsToolkit.ErrorHandling

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

[<Tests>]
let tests =
    ftestList
        "CategoryConsumer"
        [ testList
            "readBatch"
            [ ftest "should succeed" {
                  let handler =
                      fun x ->
                          printfn "received: ................ %A" x
                          Task.singleton ()

                  let readBatch =
                      CategoryConsumer.ConsumerLib.readBatch
                          (StatelessClient(cnxString))
                          (Limited 20)
                          "test"
                          handler
                          0L
                          0
                          1

                  let guid =
                      System.Guid.Parse "5F14D747-8981-4280-94CA-24825D63E7D4"

                  // writeMsg "test-readBatch" guid
                  printfn "%A" readBatch.Result
                  teardown guid
              } ]
          testList
              "ExclusiveConsumer"
              [ test "with any position should succeed" {
                    let handler =
                        fun (x: RecordedMessage) ->
                            printfn "received: ................ %A" x
                            // Expect.exists "" x.id
                            Task.singleton ()

                    let consumer =
                        CategoryConsumer.ExclusiveConsumer.ofConnectionString
                            cnxString
                            "testExclusiveConsumer"
                            "test"
                            handler
                            0L

                    let guid =
                        System.Guid.Parse "5F14D747-8981-4280-94CA-24825D63E7D5"

                    // writeMsg "test-x" guid
                    // printfn "%A" consumer.Result
                    teardown guid
                } ] ]
