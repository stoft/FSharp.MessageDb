namespace FSharp.MessageDb

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

module Int64 =
    let ofBatchSizeOption =
        function
        | None -> None
        | Some All -> Some -1L
        | Some (Limited x) -> Some x

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


module SqlLib =
    open Npgsql.FSharp

    let private toFunc (functionName: string) (parameters: (string * SqlValue) list) =
        Sql.func
        <| sprintf
            "SELECT * FROM %s (%s);"
            functionName
            (parameters
             |> List.map (fst >> sprintf "@%s")
             |> String.concat ", ")

    let private readMessage (reader: RowReader) : RecordedMessage =
        { id = System.Guid.Parse(reader.string "id")
          streamName = reader.string "stream_name"
          createdTimeUTC = reader.dateTime "time"
          version = reader.int64 "position"
          eventType = reader.string "type"
          metadata = reader.stringOrNone "metadata"
          data = reader.string "data" }

    let private int64Reader name (read: RowReader) = read.int64 name
    let private stringReader name (read: RowReader) = read.string name

    let writeMessage
        (streamName: string)
        (message: UnrecordedMessage)
        (expectedVersion: int64 option)
        (sqlProps: Sql.SqlProps)
        : ((RowReader -> int64) * Sql.SqlProps) =
        let funcName = "write_message"

        let parameters =
            [ nameof id, Sql.text (message.id.ToString())
              nameof streamName, Sql.text streamName
              nameof message.eventType, Sql.text message.eventType
              nameof message.data, Sql.jsonb message.data
              nameof message.metadata, Sql.jsonbOrNone message.metadata
              nameof expectedVersion, Sql.int64OrNone expectedVersion ]

        let func = toFunc funcName parameters sqlProps

        (int64Reader funcName, func)

    let getStreamMessages
        (streamName: string)
        (position: int64 option)
        (batchSize: BatchSize option)
        (sqlProps: Sql.SqlProps)
        =

        let parameters =
            [ nameof streamName, Sql.text streamName
              nameof position, Sql.int64OrNone position
              nameof batchSize, Sql.int64OrNone (Int64.ofBatchSizeOption batchSize)
              "condition", Sql.stringOrNone None ]

        let func =
            toFunc "get_stream_messages" parameters sqlProps

        (readMessage, func)

    let getCategoryMessages
        (categoryName: string)
        (position: int64 option)
        (batchSize: BatchSize option)
        (correlation: string option)
        (consumerGroupMember: int64 option)
        (consumberGroupSize: int64 option)
        (sqlProps: Sql.SqlProps)
        =
        let parameters =
            [ nameof categoryName, Sql.text categoryName
              nameof position, Sql.int64OrNone position
              nameof batchSize, Sql.int64OrNone (Int64.ofBatchSizeOption batchSize)
              nameof correlation, Sql.textOrNone correlation
              nameof consumerGroupMember, Sql.int64OrNone consumerGroupMember
              nameof consumberGroupSize, Sql.int64OrNone consumberGroupSize
              "condition", Sql.stringOrNone None ]

        let func =
            toFunc "get_category_messages" parameters sqlProps

        (readMessage, func)

    let getLastMessage (streamName: string) (sqlProps: Sql.SqlProps) =
        let parameters =
            [ nameof streamName, Sql.text streamName ]

        let func =
            toFunc "get_last_stream_message" parameters sqlProps

        (readMessage, func)

    let getStreamVersion (streamName: string) sqlProps =
        let parameters =
            [ nameof streamName, Sql.text streamName ]

        let funcName = "stream_version"

        let func = toFunc funcName parameters sqlProps

        (int64Reader funcName, func)

    let getCategory (streamName: string) sqlProps =
        let parameters =
            [ nameof streamName, Sql.text streamName ]

        let funcName = "category"

        let func = toFunc funcName parameters sqlProps

        (stringReader funcName, func)

    let deleteMessage (id: System.Guid) sqlProps =
        sqlProps
        |> Sql.query "DELETE FROM messages WHERE id = @id;"
        |> Sql.parameters [ nameof id, Sql.uuid id ]

module Store =
    open Npgsql.FSharp
    open FsToolkit.ErrorHandling

    let private prep
        (connection: Npgsql.NpgsqlConnection)
        (query: string)
        (parameters: List<string * SqlValue>)
        : Sql.SqlProps =

        connection
        |> Sql.existingConnection
        |> Sql.query query
        |> Sql.parameters parameters

    let private executeRowFunc
        (reader: RowReader -> 'a)
        : Npgsql.NpgsqlConnection -> string -> List<string * SqlValue> -> Task<Result<'a, exn>> =
        fun connection query params' ->
            printfn "executing row query: %s" query

            prep connection query params'
            |> (Sql.executeRowAsync reader)
            |> (Task.catch >> Task.map (Result.ofChoice))

    let private executeFunc
        (reader: RowReader -> 'a)
        : Npgsql.NpgsqlConnection -> string -> List<string * SqlValue> -> Task<Result<'a list, exn>> =
        fun connection query params' ->
            printfn "executing query: %s" query

            prep connection query params'
            |> (Sql.executeAsync reader)
            |> (Task.catch >> Task.map (Result.ofChoice))

    let private toQuery (functionName: string) (parameters: (string * SqlValue) list) : string =
        sprintf
            "SELECT * FROM %s (%s);"
            functionName
            (parameters
             |> List.map (fst >> sprintf "@%s")
             |> String.concat ", ")

    type MessageStore(connectionString: DbConnectionString) =
        // let mutable connection : Npgsql.NpgsqlConnection
        // new(connection : Npgsql.NpgsqlConnection) =
        //     this.connection <- connection
        member __.WriteMessage(streamName: string, message: UnrecordedMessage, ?expectedVersion: int64) =
            printfn "nameof: %A" []

            connectionString
            |> DbConnectionString.toString
            |> Sql.connect
            |> SqlLib.writeMessage streamName message expectedVersion
            ||> Sql.executeRowAsync
            |> TaskResult.ofTask
            |> TaskResult.catch
                (fun err ->
                    match err.Message with
                    | _ when err.Message.StartsWith("P0001: Wrong expected version") -> WrongExpectedVersion err.Message
                    | _ -> raise err)
            |> TaskResult.map (fun result -> MessageAppended(streamName, result))

        member __.GetStreamMessages(streamName: string, ?position: int64, ?batchSize: BatchSize) =
            connectionString
            |> DbConnectionString.toString
            |> Sql.connect
            |> SqlLib.getStreamMessages streamName position batchSize
            ||> Sql.executeAsync
            |> Task.map (fun msgs -> StreamMessagesRead(streamName, msgs))

        member __.GetCategoryMessages
            (
                categoryName: string,
                ?position: int64,
                ?batchSize: BatchSize,
                ?correlation: string,
                ?consumerGroupMember: int64,
                ?consumberGroupSize: int64
            ) =
            connectionString
            |> DbConnectionString.toString
            |> Sql.connect
            |> SqlLib.getCategoryMessages
                categoryName
                position
                batchSize
                correlation
                consumerGroupMember
                consumberGroupSize
            ||> Sql.executeAsync
            |> Task.map (fun msgs -> StreamMessagesRead(categoryName, msgs))

        member __.GetLastMessage(streamName: string) =
            connectionString
            |> DbConnectionString.toString
            |> Sql.connect
            |> SqlLib.getLastMessage streamName
            ||> Sql.executeAsync
            |> Task.map (fun msgs -> StreamMessagesRead(streamName, msgs))

        member __.GetStreamVersion(streamName: string) =
            connectionString
            |> DbConnectionString.toString
            |> Sql.connect
            |> SqlLib.getStreamVersion streamName
            ||> Sql.executeRowAsync

        member __.GetCategory(streamName: string) =
            connectionString
            |> DbConnectionString.toString
            |> Sql.connect
            |> SqlLib.getCategory streamName
            ||> Sql.executeRowAsync

        member __.DeleteMessage(id: System.Guid) =
            connectionString
            |> DbConnectionString.toString
            |> Sql.connect
            |> SqlLib.deleteMessage id
            |> Sql.executeNonQueryAsync

type Client = { executionType: ExecutionType }

and ExecutionType =
    | ConnectionString of string
    | Connection of Npgsql.NpgsqlConnection

type ExclusiveConsumer = ExclusiveConsumer of unit

type GroupConsumer = GroupConsumer of unit

module Client =

    let ofConnectionString: string -> Client = failwith "ni"
    let ofConnection: Npgsql.NpgsqlConnection -> Client = failwith "ni"
