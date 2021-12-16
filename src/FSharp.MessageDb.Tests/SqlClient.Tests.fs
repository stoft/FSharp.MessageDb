module SqlClient.Tests

open Expecto
open Expecto.Flip
open FSharp.MessageDb
open Npgsql.Logging
open Npgsql.FSharp

let message =
    { id = (System.Guid.NewGuid())
      eventType = "test-event"
      metadata = None
      data = "{}" }

let cnxString =
    DbConnectionString.create
        { dbHost = "localhost"
          dbUsername = "admin"
          dbPassword = "secret"
          dbPort = 11111
          dbName = "message_store"
          dbSchema = "message_store" }
    |> DbConnectionString.toString

let store =


    StatelessClient(cnxString)

let setup streamName guid =
    let input =
        { id = guid
          eventType = "test-event"
          metadata = None
          data = "{}" }

    store.WriteMessage(streamName, input).Result

let teardown id = store.DeleteMessage(id)

let testMessage id : UnrecordedMessage =
    { id = id
      eventType = "test-event"
      metadata = None
      data = "{}" }

let _ =
    NpgsqlLogManager.Provider <- ConsoleLoggingProvider(NpgsqlLogLevel.Info, true)
    NpgsqlLogManager.IsParameterLoggingEnabled <- true

[<Tests>]
let tests =
    testList
        "SqlLib"
        [ testList
              "hash"
              [ test "should work" {
                    let t =
                        (cnxString
                         |> Npgsql.FSharp.Sql.connect
                         |> SqlClient.hash64 "test-hash1")

                    Expect.equal "" -7674236520607344939L t.Result
                } ]
          testList
              "doTryGetAdvisoryLock"
              [ test "should return true for first and false for second" {
                    use connection =
                        cnxString
                        |> Npgsql.FSharp.Sql.connect
                        |> Npgsql.FSharp.Sql.createConnection

                    let props = connection |> Npgsql.FSharp.Sql.existingConnection

                    let first = SqlLib.doTryGetAdvisoryLock -7674236520607344939L props

                    Expect.isTrue "" first.Result

                    let second =
                        SqlLib.doTryGetAdvisoryLock -7674236520607344939L (cnxString |> Npgsql.FSharp.Sql.connect)

                    Expect.isFalse "" second.Result

                } ]
          testList
              "doTryGetAdvisoryLock"
              [ test "should succeed" {
                    let result =
                        (cnxString
                         |> Npgsql.FSharp.Sql.connect
                         |> SqlLib.doTryGetAdvisoryLock 99999999L)
                            .Result

                    Expect.isTrue "" result
                } ]

          testList
              "writeMessage"
              [ test "with any position should succeed" {
                    let guid = System.Guid.Parse "ce83d845-2620-475c-abb6-94f727990ee8"

                    let input =
                        { id = guid
                          eventType = "test-event"
                          metadata = None
                          data = "{}" }

                    let result = store.WriteMessage("test-stream1", input).Result

                    Expect.wantOk "" result |> ignore
                    teardown guid |> ignore
                }

                test "with position 0 on non-existant stream should fail" {
                    let guid = System.Guid.Parse "b81edb3d-a011-4214-8131-00a4a0deb6a7"

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
                                Version 0L
                            )
                            .Result

                    Expect.wantError "should fail" result2
                    |> Expect.equal
                        ""
                        (WrongExpectedVersion
                            "P0001: Wrong expected version: 0 (Stream: test-stream2, Stream Version: -1)")

                    guid |> teardown |> ignore
                }
                test "writing with position NoStream on non-existant stream should succeed" {
                    let guid = System.Guid.Parse "b81edb3d-a011-4214-8131-00a4a0deb6a8"

                    let input = testMessage guid

                    let result2 =
                        store
                            .WriteMessage(
                                "test-stream3",
                                input,
                                NoStream
                            )
                            .Result

                    Expect.wantOk "should succeed" result2 |> ignore

                    guid |> teardown |> ignore
                }

                test "with position NoStream on existing stream should fail with WrongExpectedVersion" {
                    let guid = System.Guid.Parse "f5014710-c87f-4b68-96ae-ceb76e2c598e"

                    store
                        .WriteMessage(
                            "test-nostreamOnExistingShouldFail",
                            testMessage guid,
                            NoStream
                        )
                        .Result
                    |> ignore

                    let guid2 = System.Guid.Parse "a5014710-c87f-4b68-96ae-ceb76e2c598e"

                    let result2 =
                        store
                            .WriteMessage(
                                "test-nostreamOnExistingShouldFail",
                                testMessage guid2,
                                NoStream
                            )
                            .Result

                    Expect.wantError "should fail" result2 |> ignore

                    guid |> teardown |> ignore
                    guid2 |> teardown |> ignore
                } ]
          testList
              "get_stream_messages"
              [ test "get_stream_messages from non-existant stream should result in empty list" {
                    let result =
                        store
                            .GetStreamMessages(
                                "test-getStreamMessageFromNonExistantStream"
                            )
                            .Result

                    Expect.equal "" [] result
                } ]
          testList
              "get_last_message"
              [ test "from empty/non-existant stream should result in empty list" {
                    let result =
                        store
                            .GetLastMessage(
                                "test-nonExistantStream"
                            )
                            .Result

                    Expect.isNone "" result
                }
                test "from stream should result in Some message" {
                    let guid = System.Guid.Parse "A26EFFE2-F9A8-4EF1-860F-7A73B06D0FC6"

                    setup "test-oneMessageStream" guid |> ignore

                    let result =
                        store
                            .GetLastMessage(
                                "test-oneMessageStream"
                            )
                            .Result

                    teardown guid |> ignore

                    Expect.isSome "" result

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
                    let guid = System.Guid.Parse "1d49e0d9-1007-40c0-9a29-1f4a4f24281a"

                    store
                        .WriteMessage(
                            "testDeleteMessage-1",
                            testMessage guid
                        )
                        .Result
                    |> ignore

                    let result = (teardown guid).Result
                    result |> Expect.equal "" 1

                } ]
          //   testList
          //       "multi"
          //       [ ftest "foo" {
          //             cnxString
          //             |> Sql.connect
          //             |> Sql.executeTransaction [ "set role message_store;", [ [] ]
          //                                         "SELECT * FROM message_store.write_message('e07ddf02-b682-4b1f-8535-a8e02181f08b', 'test-123', 'event-type','{}', NULL, NULL);",
          //                                         [ [] ]
          //                                         "SELECT * FROM message_store.write_message('f07ddf02-b682-4b1f-8535-a8e02181f08b', 'test-123', 'event-type','{}', NULL, -1);",
          //                                         [ [] ] ]
          //             |> List.map (printfn "%A")

          //             ()
          //         } ]
          ]
