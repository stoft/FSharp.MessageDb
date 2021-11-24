namespace FSharp.MessageDb.SqlClient

// Copied from https://github.com/sebfia/message-db-event-store/blob/master/src/Contracts/Contracts.fs
[<AutoOpen>]
module Contracts =
    open System

    type RecordedMessage =
        { id: Guid
          streamName: string
          createdTimeUTC: DateTime
          version: int64
          eventType: string
          metadata: string option
          data: string }

    type UnrecordedMessage =
        { id: Guid
          eventType: string
          metadata: string option
          data: string }

    type BatchSize =
        | All
        | Limited of int64

    type SuccessResponse =
        | MessageAppended of streamName: string * messageNumber: int64
        | MessagesAppended of streamName: string * lastMessageNumber: int64
        | StreamMessagesRead of streamName: string * recordedMessages: RecordedMessage array
        | CategoryMessagesRead of categoryName: string * recordedMessages: RecordedMessage array

    type ErrorResponse = WrongExpectedVersion of errMessage: string

    type Response = Result<SuccessResponse, string>

    type Request =
        | AppendMessage of streamName: string * expectedVersion: int64 * unrecordedEvent: UnrecordedMessage
        | AppendMessages of streamName: string * expectedEventNumber: int64 * unrecordedEvents: UnrecordedMessage array
        | ReadStreamMessages of streamName: string * fromEventNumber: int64 option * numEvents: BatchSize
        | ReadCategoryMessages of categoryName: string * fromEventNumber: int64 option * numEvents: BatchSize

type DbConnectionString = internal DbConnectionString of string

module DbConnectionString =
    type DbConfig =
        { dbUsername: string
          dbPassword: string
          dbName: string
          dbHost: string
          dbPort: int
          dbSchema: string }

    let create
        ({ dbUsername = username
           dbPassword = password
           dbName = dbName
           dbHost = host
           dbPort = port
           dbSchema = schema })
        =
        DbConnectionString
            $"Server=%s{host};Port=%d{port};Database={dbName};User Id=%s{username};Password=%s{password};Search Path=%s{schema}"

    let toString: DbConnectionString -> string = fun (DbConnectionString s) -> s

module Store =
    open System.Threading.Tasks
    open Npgsql.FSharp
    open FsToolkit.ErrorHandling

    (*
write_message
get_stream_messages
get_category_messages
get_last_message
stream_version
id
cardinal_id
category
is_category
acquire_lock
hash_64
message_store_version
    *)

    type MessageStore(connectionString: DbConnectionString) =
        // /// <summary>
        // ///
        // /// </summary>
        // /// <param name="streamName"></param>
        // /// <param name="position"></param>
        // /// <param name="batchSize"></param>
        // /// <param name="cancellationToken"></param>
        // /// <typeparam name="'d"></typeparam>
        // /// <returns></returns>
        // member __.GetStreamMessages
        //     (
        //         streamName: string,
        //         ?position: int64,
        //         ?batchSize: int64,
        //         ?cancellationToken: CancellationToken
        //     ) =
        //     async {
        //         let token =
        //             defaultArg cancellationToken CancellationToken.None

        //         use connection = new NpgsqlConnection(connectionString)

        //         do!
        //             Sql.setRole connection token "message_store"
        //             |> Async.Ignore

        //         let func =
        //             [ Sql.stringParameter "stream_name" streamName
        //               |> Some
        //               Option.map (fun v -> Sql.int64Parameter "position" v) position
        //               Option.map (fun v -> Sql.int64Parameter "batch_size" v) batchSize ]
        //             |> List.choose id
        //             |> Sql.createFunc connection "get_stream_messages"

        //         return! Sql.executeQueryAsync connection func token readMessage
        //     }
        (*
        write_message(
              id varchar,
              stream_name varchar,
              type varchar,
              data jsonb,
              metadata jsonb DEFAULT NULL,
              expected_version bigint DEFAULT NULL
        )
        *)
        member __.WriteMessage(streamName: string, message: UnrecordedMessage, ?expectedVersion: int64) =
            asyncResult {
                let prep =
                    connectionString
                    |> DbConnectionString.toString
                    |> Sql.connect
                    |> Sql.query
                        """
                        SELECT write_message(@id, @streamName, @type, @data, @metadata, @expectedVersion);
                        """
                    |> Sql.parameters [ "id", Sql.text (message.id.ToString())
                                        "streamName", Sql.text streamName
                                        "type", Sql.text message.eventType
                                        "data", Sql.jsonb message.data
                                        "metadata", Sql.jsonbOrNone message.metadata
                                        "expectedVersion", Sql.int64OrNone expectedVersion ]

                return!
                    prep
                    |> Sql.executeRowAsync (fun read -> read.int64 "write_message")
                    |> Task.catch
                    |> Task.map
                        (function
                        | Choice1Of2 result -> Ok <| MessageAppended(streamName, result)
                        | Choice2Of2 err ->
                            match err.Message with
                            | _ when err.Message.StartsWith("P0001: Wrong expected version") ->
                                Error(WrongExpectedVersion err.Message)
                            | _ -> raise err)
            }
// let readMessage (reader: Sql.RowReader) =
//     { Id = Guid.Parse(reader.string "id")
//       StreamName = reader.string "stream_name"
//       CreatedTimeUTC = reader.dateTime "time"
//       Version = reader.int64 "position"
//       EventType = reader.string "type"
//       Metadata = reader.stringOrNone "metadata"
//       Data = reader.string "data" }

// let createWriteFunc connection streamName (eventId: Guid) eventType data metadata expectedVersion =
//     [ streamName
//       |> Sql.stringParameter "stream_name"
//       |> Some
//       (eventId.ToString())
//       |> Sql.stringParameter "id"
//       |> Some
//       eventType |> Sql.stringParameter "type" |> Some
//       data |> Sql.jsonParameter "data" |> Some
//       Option.map (fun v -> Sql.jsonParameter "metadata" v) metadata
//       Option.map (fun v -> Sql.int64Parameter "expected_version" v) expectedVersion ]
//     |> List.choose id
//     |> Sql.createFunc connection "write_message"

// member __.GetStreamMessages
//     (
//         streamName: string,
//         ?position: int64,
//         ?batchSize: int64,
//         ?cancellationToken: CancellationToken
//     ) =
//     async {
//         let token =
//             defaultArg cancellationToken CancellationToken.None

//         use connection = new NpgsqlConnection(connectionString)

//         do!
//             Sql.setRole connection token "message_store"
//             |> Async.Ignore

//         let func =
//             [ Sql.stringParameter "stream_name" streamName
//               |> Some
//               Option.map (fun v -> Sql.int64Parameter "position" v) position
//               Option.map (fun v -> Sql.int64Parameter "batch_size" v) batchSize ]
//             |> List.choose id
//             |> Sql.createFunc connection "get_stream_messages"

//         return! Sql.executeQueryAsync connection func token readMessage
//     }

// member __.WriteStreamMessage
//     (
//         streamName: string,
//         message: UnrecordedMessage,
//         ?expectedVersion: int64,
//         ?cancellationToken: CancellationToken
//     ) =
//     async {
//         let token =
//             defaultArg cancellationToken CancellationToken.None

//         use connection = new NpgsqlConnection(connectionString)

//         do!
//             Sql.setRole connection token "message_store"
//             |> Async.Ignore

//         let func =
//             createWriteFunc
//                 connection
//                 streamName
//                 message.Id
//                 message.Data
//                 message.Data
//                 message.Metadata
//                 expectedVersion

//         match! Sql.executeScalarAsync<int64> connection [| func |] token with
//         | Error exn -> return Error exn
//         | Ok lst -> return lst |> List.head |> Ok
//     }

// member __.WriteStreamMessages
//     (
//         streamName: string,
//         messages: UnrecordedMessage array,
//         ?expectedVersion: int64,
//         ?cancellationToken: CancellationToken
//     ) =
//     async {
//         let token =
//             defaultArg cancellationToken CancellationToken.None

//         use connection = new NpgsqlConnection(connectionString)

//         do!
//             Sql.setRole connection token "message_store"
//             |> Async.Ignore

//         let createWriteFunc' = createWriteFunc connection streamName

//         let funcs =
//             messages
//             |> Array.mapi
//                 (fun i m ->
//                     createWriteFunc'
//                         m.Id
//                         m.EventType
//                         m.Data
//                         m.Metadata
//                         (if i > 0 then None else expectedVersion))

//         return! Sql.executeScalarAsync connection funcs token
//     }

// member __.GetLastStreamMessage(streamName: string, ?cancellationToken) =
//     async {
//         let token =
//             defaultArg cancellationToken CancellationToken.None

//         use connection = new NpgsqlConnection(connectionString)

//         do!
//             Sql.setRole connection token "message_store"
//             |> Async.Ignore

//         let func =
//             [ Sql.stringParameter "stream_name" streamName ]
//             |> Sql.createFunc connection "get_last_stream_message"

//         match! Sql.executeQueryAsync connection func token readMessage with
//         | Ok lst -> return Ok(lst |> List.tryHead)
//         | Error exn -> return Error exn
//     }

// member __.GetLastStreamVersion(streamName: string, ?cancellationToken) =
//     async {
//         let token =
//             defaultArg cancellationToken CancellationToken.None

//         use connection = new NpgsqlConnection(connectionString)

//         do!
//             Sql.setRole connection token "message_store"
//             |> Async.Ignore

//         let func =
//             [ Sql.stringParameter "stream_name" streamName ]
//             |> Sql.createFunc connection "stream_version"

//         match! Sql.executeQueryAsync connection func token (fun r -> r.int64OrNone "") with
//         | Ok lst ->
//             return
//                 Ok(
//                     lst
//                     |> List.head
//                     |> function
//                         | Some i -> i
//                         | _ -> -1L
//                 )
//         | Error exn -> return Error exn
//     }
