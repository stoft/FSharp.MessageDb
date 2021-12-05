module SqlClient.Tests

open Expecto
open Expecto.Flip
open FSharp.MessageDb
open Npgsql.Logging

let message =
    { id = (System.Guid.NewGuid())
      eventType = "test-event"
      metadata = None
      data = "{}" }

let store =
    StatelessClient.StatelessClient(
        DbConnectionString.create
            { dbHost = "localhost"
              dbUsername = "admin"
              dbPassword = "secret"
              dbPort = 11111
              dbName = "message_store"
              dbSchema = "message_store" }
    )

let setup = ()

let teardown id = store.DeleteMessage(id)

let testMessage id : UnrecordedMessage =
    { id = id
      eventType = "test-event"
      metadata = None
      data = "{}" }

let _ =
    NpgsqlLogManager.Provider <- ConsoleLoggingProvider(NpgsqlLogLevel.Debug, true)
    NpgsqlLogManager.IsParameterLoggingEnabled <- true

[<Tests>]
let tests =
    testList
        "SqlLib"
        [ testList
            "writeMessage"
            [ test "any position should succeed" {
                let guid =
                    System.Guid.Parse "ce83d845-2620-475c-abb6-94f727990ee8"

                let input =
                    { id = guid
                      eventType = "test-event"
                      metadata = None
                      data = "{}" }

                let result =
                    store.WriteMessage("test-stream1", input).Result

                Expect.wantOk "" result |> ignore
                teardown guid |> ignore
              }

              test "writing with position 0 on non-existant stream should fail" {
                  let guid =
                      System.Guid.Parse "b81edb3d-a011-4214-8131-00a4a0deb6a7"

                  let input =
                      { id = guid
                        eventType = "test-event"
                        metadata = None
                        data = "{}" }

                  let result2 =
                      store
                          .WriteMessage(
                              "test-stream2",
                              input,
                              0L
                          )
                          .Result

                  Expect.wantError "should fail" result2
                  |> Expect.equal
                      ""
                      (WrongExpectedVersion
                          "P0001: Wrong expected version: 0 (Stream: test-stream2, Stream Version: -1)")

                  guid |> teardown |> ignore
              }
              test "writing with position -1 on non-existant stream should succeed" {
                  let guid =
                      System.Guid.Parse "b81edb3d-a011-4214-8131-00a4a0deb6a8"

                  let input =
                      { id = guid
                        eventType = "test-event"
                        metadata = None
                        data = "{}" }

                  let result2 =
                      store
                          .WriteMessage(
                              "test-stream3",
                              input,
                              -1L
                          )
                          .Result

                  Expect.wantOk "should succeed" result2 |> ignore

                  guid |> teardown |> ignore
              } ]

          testList
              "get_stream_messages"
              [ test "get_stream_messages" {

                    let result =
                        store
                            .GetStreamMessages(
                                "test-writeMessage1"
                            )
                            .Result

                    ()
                } ]

          testList
              "get_category_messages"
              [ test "by category name" {
                    let result = store.GetCategoryMessages("test").Result
                    ()
                } ]

          testList
              "delete_message"
              [ test "should succeed" {
                    let guid =
                        System.Guid.Parse "1d49e0d9-1007-40c0-9a29-1f4a4f24281a"

                    store
                        .WriteMessage(
                            "testDeleteMessage-1",
                            testMessage guid
                        )
                        .Result
                    |> ignore

                    let result = (teardown guid).Result
                    result |> Expect.equal "" 1

                } ] ]
