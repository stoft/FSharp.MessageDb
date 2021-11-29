namespace FSharp.MessageDb.SqlClient

open System.Threading.Tasks

// ref: https://github.com/sebfia/message-db-event-store
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
        | StreamMessagesRead of streamName: string * recordedMessages: RecordedMessage list
        | CategoryMessagesRead of categoryName: string * recordedMessages: RecordedMessage list

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
    open Npgsql.FSharp
    open FsToolkit.ErrorHandling

    let private readMessage (reader: RowReader) : RecordedMessage =
        { id = System.Guid.Parse(reader.string "id")
          streamName = reader.string "stream_name"
          createdTimeUTC = reader.dateTime "time"
          version = reader.int64 "position"
          eventType = reader.string "type"
          metadata = reader.stringOrNone "metadata"
          data = reader.string "data" }

    let private prep
        (cnxString: DbConnectionString)
        (query: string)
        (parameters: List<string * SqlValue>)
        : Sql.SqlProps =
        cnxString
        |> DbConnectionString.toString
        |> Sql.connect
        |> Sql.query query
        |> Sql.parameters parameters

    let private writeRowFunc
        (reader: RowReader -> 'a)
        : DbConnectionString -> string -> List<string * SqlValue> -> Task<Result<'a, exn>> =
        fun cnxString query params' ->
            prep cnxString query params'
            |> (Sql.executeRowAsync reader)
            |> (Task.catch >> Task.map (Result.ofChoice))

    let private executeFunc
        (reader: RowReader -> 'a)
        : DbConnectionString -> string -> List<string * SqlValue> -> Task<Result<'a list, exn>> =
        fun cnxString query params' ->
            prep cnxString query params'
            |> (Sql.executeAsync reader)
            |> (Task.catch >> Task.map (Result.ofChoice))

    let toQuery (functionName: string) (parameters: (string * SqlValue) list) : string =
        sprintf
            "SELECT %s (%s);"
            functionName
            (parameters
             |> List.map (fst >> sprintf "@%s")
             |> String.concat ", ")

    type MessageStore(connectionString: DbConnectionString) =
        member __.WriteMessage(streamName: string, message: UnrecordedMessage, ?expectedVersion: int64) =
            printfn "nameof: %A" []

            let funcName = "write_message"

            let parameters =
                [ nameof id, Sql.text (message.id.ToString())
                  nameof streamName, Sql.text streamName
                  nameof message.eventType, Sql.text message.eventType
                  nameof message.data, Sql.jsonb message.data
                  nameof message.metadata, Sql.jsonbOrNone message.metadata
                  nameof expectedVersion, Sql.int64OrNone expectedVersion ]

            let query = toQuery funcName parameters

            let reader: RowReader -> int64 =
                fun (read: RowReader) -> read.int64 funcName

            asyncResult {
                return!
                    writeRowFunc reader connectionString query parameters
                    |> TaskResult.map (fun result -> MessageAppended(streamName, result))
                    |> TaskResult.mapError
                        (fun err ->
                            match err.Message with
                            | _ when err.Message.StartsWith("P0001: Wrong expected version") ->
                                Error(WrongExpectedVersion err.Message)
                            | _ -> raise err)
            }

        member __.GetStreamMessages(streamName: string, ?position: int64, ?batchSize: BatchSize) =
            let batchSize' =
                match batchSize with
                | None -> None
                | Some All -> Some -1L
                | Some (Limited x) -> Some x

            let parameters =
                [ nameof streamName, Sql.text streamName
                  nameof position, Sql.int64OrNone position
                  nameof batchSize, Sql.int64OrNone batchSize'
                  "condition", Sql.stringOrNone None ]

            let query = toQuery "get_stream_messages" parameters

            asyncResult {
                return!
                    executeFunc readMessage connectionString query parameters
                    |> TaskResult.map (fun msgs -> StreamMessagesRead(streamName, msgs))
                    |> TaskResult.mapError
                        (fun err ->
                            match err.Message with
                            | _ when err.Message.StartsWith("P0001: Wrong expected version") ->
                                Error(WrongExpectedVersion err.Message)
                            | _ -> raise err)
            }

        member __.GetCategoryMessages
            (
                categoryName: string,
                ?position: int64,
                ?batchSize: BatchSize,
                ?correlation: string,
                ?consumerGroupMember: int64,
                ?consumberGroupSize: int64
            ) =
            let batchSize' =
                match batchSize with
                | None -> None
                | Some All -> Some -1L
                | Some (Limited x) -> Some x

            let parameters =
                [ nameof categoryName, Sql.text categoryName
                  nameof position, Sql.int64OrNone position
                  nameof batchSize, Sql.int64OrNone batchSize'
                  nameof correlation, Sql.textOrNone correlation
                  nameof consumerGroupMember, Sql.int64OrNone consumerGroupMember
                  nameof consumberGroupSize, Sql.int64OrNone consumberGroupSize
                  "condition", Sql.stringOrNone None ]

            let query =
                toQuery "get_category_messages" parameters

            asyncResult {
                return!
                    executeFunc readMessage connectionString query parameters
                    |> TaskResult.map (fun msgs -> StreamMessagesRead(categoryName, msgs))
                    |> TaskResult.mapError
                        (fun err ->
                            match err.Message with
                            | _ when err.Message.StartsWith("P0001: Wrong expected version") ->
                                Error(WrongExpectedVersion err.Message)
                            | _ -> raise err)
            }

        member __.GetLastMessage(streamName: string) =
            let parameters =
                [ nameof streamName, Sql.text streamName ]

            let query =
                toQuery "get_last_stream_message" parameters

            asyncResult {
                return!
                    executeFunc readMessage connectionString query parameters
                    |> TaskResult.map (fun msgs -> StreamMessagesRead(streamName, msgs))
                    |> TaskResult.mapError
                        (fun err ->
                            match err.Message with
                            | _ when err.Message.StartsWith("P0001: Wrong expected version") ->
                                Error(WrongExpectedVersion err.Message)
                            | _ -> raise err)
            }

        member __.GetStreamVersion(streamName: string) =
            let parameters =
                [ nameof streamName, Sql.text streamName ]

            let funcName = "stream_version"

            let query = toQuery funcName parameters

            let reader: RowReader -> int64 =
                fun (read: RowReader) -> read.int64 funcName

            asyncResult {
                return!
                    executeFunc reader connectionString query parameters
                    |> TaskResult.mapError
                        (fun err ->
                            match err.Message with
                            | _ when err.Message.StartsWith("P0001: Wrong expected version") ->
                                Error(WrongExpectedVersion err.Message)
                            | _ -> raise err)
            }

        member __.GetCategory(streamName: string) =
            let parameters =
                [ nameof streamName, Sql.text streamName ]

            let query = toQuery "stream_version" parameters

            asyncResult {
                return!
                    executeFunc readMessage connectionString query parameters
                    |> TaskResult.map (fun msgs -> StreamMessagesRead(streamName, msgs))
                    |> TaskResult.mapError
                        (fun err ->
                            match err.Message with
                            | _ when err.Message.StartsWith("P0001: Wrong expected version") ->
                                Error(WrongExpectedVersion err.Message)
                            | _ -> raise err)
            }
