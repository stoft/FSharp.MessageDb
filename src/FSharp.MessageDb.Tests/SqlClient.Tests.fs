module SqlClient.Tests

open Expecto
open Expecto.Flip
open FSharp.MessageDb.SqlClient

let message =
    { id = (System.Guid.NewGuid())
      eventType = "test-event"
      metadata = None
      data = "{}" }

[<Tests>]
let tests =
    testList
        "list"
        [ testAsync "write_message" {
              let store =
                  Store.MessageStore(
                      DbConnectionString.create
                          { dbHost = "localhost"
                            dbUsername = "admin"
                            dbPassword = "secret"
                            dbPort = 11111
                            dbName = "message_store"
                            dbSchema = "message_store" }
                  )


              async {
                  let input =
                      { id = (System.Guid.NewGuid())
                        eventType = "test-event"
                        metadata = None
                        data = "{}" }

                  printfn $"{input}"

                  let! result = store.WriteMessage("test-writeMessage1", input)

                  Expect.wantOk "" result |> ignore

                  let! result2 = store.WriteMessage("test-writeMessage2", input, 0L)
                  Expect.wantError "should fail" result2 |> ignore

              }
              |> Async.RunSynchronously
              |> ignore

          } ]
