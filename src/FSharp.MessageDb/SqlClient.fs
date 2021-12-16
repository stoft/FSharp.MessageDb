namespace FSharp.MessageDb

open System.Threading.Tasks
open FsToolkit.ErrorHandling
open Npgsql.FSharp

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
          globalPosition: int64
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
        | Limited of int

    type StreamMessages = StreamMessages of recordedMessages: RecordedMessage list
    type CategoryMessages = CategoryMessages of recordedMessages: RecordedMessage list
    type ErrorResponse = WrongExpectedVersion of errMessage: string
    type GlobalPosition = GlobalPosition of position: int64

    type StreamVersion =
        | Any
        | NoStream
        | Version of int64

type DbConnectionString = internal DbConnectionString of string

module internal Result =
    let catch f x =
        try
            f x |> Ok
        with
        | exn -> Error exn

module internal TaskResult =
    let bindError: ('Error -> Task<Result<'a, 'Error2>>) -> Task<Result<'a, 'Error>> -> Task<Result<'a, 'Error2>> =
        fun fError ->
            Task.bind (function
                | Ok v -> TaskResult.ok v
                | Error e -> fError e)

module internal Option =
    let ofBool: 'a -> bool -> 'a option = fun v b -> if b then Some v else None

module Int64 =
    let ofBatchSizeOption =
        function
        | None -> None
        | Some All -> Some -1L
        | Some (Limited x) -> Some(int64 x)

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

module StreamName =
    let ofCategoryName (categoryName: string) (suffixId: string) = $"%s{categoryName}-%s{suffixId}"

module SqlLib =
    type Parameter = (string * SqlValue)

    let private toStatement (functionName: string) (parameters: Parameter list) =
        sprintf
            "SELECT * FROM %s(%s);"
            functionName
            (parameters
             |> List.map (fst >> sprintf "@%s")
             |> String.concat ", "),
        parameters

    let private toFunc (functionName: string) (parameters: Parameter list) =
        Sql.query
        <| sprintf
            "SET role message_store; SELECT * FROM %s(%s);"
            functionName
            (parameters
             |> List.map (fst >> sprintf "@%s")
             |> String.concat ", ")
        >> Sql.parameters parameters

    let private readMessage (reader: RowReader) : RecordedMessage =
        { id = System.Guid.Parse(reader.string "id")
          streamName = reader.string "stream_name"
          createdTimeUTC = reader.dateTime "time"
          version = reader.int64 "position"
          globalPosition = reader.int64 "global_position"
          eventType = reader.string "type"
          metadata = reader.stringOrNone "metadata"
          data = reader.string "data" }

    let private int64Reader name (read: RowReader) = read.int64 name
    let private stringReader name (read: RowReader) = read.string name
    let private boolReader name (read: RowReader) = read.bool name

    let doTryGetAdvisoryLock (key: int64) : Sql.SqlProps -> Task<bool> =
        Sql.query "SELECT * from pg_try_advisory_lock(@key)"
        >> Sql.parameters [ "key", Sql.int64 key ]
        >> Sql.executeAsync (fun read -> read.bool "pg_try_advisory_lock")
        >> Task.map List.head

    let matchWrongExpectedVersion (err: exn) =
        match err.Message with
        | _ when err.Message.StartsWith("P0001: Wrong expected version") -> WrongExpectedVersion err.Message
        | _ -> raise err

    let hash64 (streamName: string) (sqlProps: Sql.SqlProps) : ((RowReader -> int64) * Sql.SqlProps) =
        let funcName = "hash_64"

        let parameters = [ nameof streamName, Sql.text streamName ]

        let func = toFunc funcName parameters sqlProps
        (int64Reader funcName, func)

    let private toWriteMessage
        (streamName: string)
        (message: UnrecordedMessage)
        (expectedVersion: int64 option)
        : string * Parameter list =
        "write_message",
        [ nameof id, Sql.text (message.id.ToString())
          nameof streamName, Sql.text streamName
          nameof message.eventType, Sql.text message.eventType
          nameof message.data, Sql.jsonb message.data
          nameof message.metadata, Sql.jsonbOrNone message.metadata
          nameof expectedVersion, Sql.int64OrNone expectedVersion ]

    let writeMessage
        (streamName: string)
        (message: UnrecordedMessage)
        (expectedVersion: int64 option)
        (sqlProps: Sql.SqlProps)
        : ((RowReader -> int64) * Sql.SqlProps) =
        let (funcName, parameters) = toWriteMessage streamName message expectedVersion

        let func = toFunc funcName parameters sqlProps

        (int64Reader funcName, func)

    let writeMessages (streamName: string) (messages: UnrecordedMessage list) (expectedVersion: int64 option) =
        // let queries =
        List.map (fun (a, b) -> a, List.singleton b)
        <| match expectedVersion with
           | None -> List.map (fun msg -> toWriteMessage streamName msg None) messages
           | Some version ->
               messages
               |> List.indexed
               |> List.map (fun (idx, msg) -> toWriteMessage streamName msg (Some <| version + (int64 idx)))

    // Sql.executeTransactionAsync queries sqlProps

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

        let func = toFunc "get_stream_messages" parameters sqlProps

        (readMessage, func)

    let getCategoryMessages
        (categoryName: string)
        (position: int64 option)
        (batchSize: BatchSize option)
        (correlation: string option)
        (consumerGroupMember: int64 option)
        (consumerGroupSize: int64 option)
        (sqlProps: Sql.SqlProps)
        =
        let parameters =
            [ nameof categoryName, Sql.text categoryName
              nameof position, Sql.int64OrNone position
              nameof batchSize, Sql.int64OrNone (Int64.ofBatchSizeOption batchSize)
              nameof correlation, Sql.textOrNone correlation
              nameof consumerGroupMember, Sql.int64OrNone consumerGroupMember
              nameof consumerGroupSize, Sql.int64OrNone consumerGroupSize
              "condition", Sql.stringOrNone None ]

        let func = toFunc "get_category_messages" parameters sqlProps

        (readMessage, func)

    let getLastMessage (streamName: string) (sqlProps: Sql.SqlProps) =
        let parameters = [ nameof streamName, Sql.text streamName ]

        let func = toFunc "get_last_stream_message" parameters sqlProps

        (readMessage, func)

    let getStreamVersion (streamName: string) sqlProps =
        let parameters = [ nameof streamName, Sql.text streamName ]

        let funcName = "stream_version"

        let func = toFunc funcName parameters sqlProps

        (int64Reader funcName, func)

    let getCategory (streamName: string) sqlProps =
        let parameters = [ nameof streamName, Sql.text streamName ]

        let funcName = "category"

        let func = toFunc funcName parameters sqlProps

        (stringReader funcName, func)

    let deleteMessage (id: System.Guid) sqlProps =
        sqlProps
        |> Sql.query "DELETE FROM messages WHERE id = @id;"
        |> Sql.parameters [ nameof id, Sql.uuid id ]

module SqlClient =
    let hash64 streamName sqlProps =
        sqlProps
        |> SqlLib.hash64 streamName
        ||> Sql.executeRowAsync

    let doTryGetAdvisoryLockOnStreamName streamName sqlProps =
        hash64 streamName sqlProps
        |> Task.bind (fun hash -> SqlLib.doTryGetAdvisoryLock hash sqlProps)

    let writeMessageAsync streamName message expectedVersion sqlProps =
        sqlProps
        |> SqlLib.writeMessage streamName message expectedVersion
        ||> Sql.executeRowAsync
        |> TaskResult.ofTask
        |> TaskResult.catch (SqlLib.matchWrongExpectedVersion)
        |> TaskResult.map GlobalPosition

    let writeMessagesAsync streamName messages expectedVersion sqlProps =
        sqlProps
        |> Sql.executeTransactionAsync (SqlLib.writeMessages streamName messages expectedVersion)

    let getStreamMessages streamName position batchSize sqlProps =
        sqlProps
        |> SqlLib.getStreamMessages streamName position batchSize
        ||> Sql.executeAsync

    let getCategoryMessages categoryName position batchSize correlation consumerGroupMember consumerGroupSize sqlProps =
        sqlProps
        |> SqlLib.getCategoryMessages categoryName position batchSize correlation consumerGroupMember consumerGroupSize
        ||> Sql.executeAsync
    // |> Task.map StreamMessages

    let getLastMessage streamName sqlProps =
        sqlProps
        |> SqlLib.getLastMessage streamName
        ||> Sql.executeAsync
        |> Task.map List.tryHead

    let getStreamVersion streamName sqlProps =
        sqlProps
        |> SqlLib.getStreamVersion streamName
        ||> Sql.executeRowAsync

type StatelessClient(connectionString: string) =
    member __.WriteMessage(streamName: string, message: UnrecordedMessage, ?expectedVersion: StreamVersion) =
        let expectedVersion' =
            (match expectedVersion with
             | Some NoStream -> Some -1L
             | Some (Version n) -> Some n
             | Some Any -> None
             | None -> None)

        connectionString
        |> Sql.connect
        |> SqlClient.writeMessageAsync streamName message expectedVersion'

    member __.WriteMessages(streamName: string, messages: UnrecordedMessage list, ?expectedVersion: StreamVersion) =
        let expectedVersion' =
            (match expectedVersion with
             | Some NoStream -> Some -1L
             | Some (Version n) -> Some n
             | Some Any -> None
             | None -> None)

        connectionString
        |> Sql.connect
        |> SqlClient.writeMessagesAsync streamName messages expectedVersion'

    member __.GetStreamMessages(streamName: string, ?position: int64, ?batchSize: BatchSize) =
        connectionString
        |> Sql.connect
        |> SqlClient.getStreamMessages streamName position batchSize

    member __.GetCategoryMessages
        (
            categoryName: string,
            ?position: int64,
            ?batchSize: BatchSize,
            ?correlation: string,
            ?consumerGroupMember: int64,
            ?consumerGroupSize: int64
        ) =
        connectionString
        |> Sql.connect
        |> SqlClient.getCategoryMessages
            categoryName
            position
            batchSize
            correlation
            consumerGroupMember
            consumerGroupSize

    member __.GetLastMessage(streamName: string) =
        connectionString
        |> Sql.connect
        |> SqlClient.getLastMessage streamName

    member __.TryStreamHead(streamName: string) =
        connectionString
        |> Sql.connect
        |> SqlClient.getStreamMessages streamName (Some 0L) (Some <| Limited 1)
        |> Task.map List.tryHead

    member __.GetStreamVersion(streamName: string) =
        connectionString
        |> Sql.connect
        |> SqlClient.getStreamVersion streamName

    member __.GetCategory(streamName: string) =
        connectionString
        |> Sql.connect
        |> SqlLib.getCategory streamName
        ||> Sql.executeRowAsync

    member __.DeleteMessage(id: System.Guid) =
        connectionString
        |> Sql.connect
        |> SqlLib.deleteMessage id
        |> Sql.executeNonQueryAsync

type Client = { executionType: ExecutionType }

and ExecutionType =
    | ConnectionString of string
    | Connection of Npgsql.NpgsqlConnection

type Synchronicity =
    | Sync
    | EagerAsyncTasks
// | LazyAsyncs


type StatefulClient(connection: Npgsql.NpgsqlConnection) =
    member __.WriteMessage(streamName: string, message: UnrecordedMessage, ?expectedVersion: int64) =
        connection
        |> Sql.existingConnection
        |> SqlClient.writeMessageAsync streamName message expectedVersion

    member __.GetStreamMessages(streamName: string, ?position: int64, ?batchSize: BatchSize) =
        connection
        |> Sql.existingConnection
        |> SqlClient.getStreamMessages streamName position batchSize

    member __.GetCategoryMessages
        (
            categoryName: string,
            ?position: int64,
            ?batchSize: BatchSize,
            ?correlation: string,
            ?consumerGroupMember: int64,
            ?consumerGroupSize: int64
        ) =
        connection
        |> Sql.existingConnection
        |> SqlClient.getCategoryMessages
            categoryName
            position
            batchSize
            correlation
            consumerGroupMember
            consumerGroupSize

    member __.GetLastMessage(streamName: string) =
        connection
        |> Sql.existingConnection
        |> SqlClient.getLastMessage streamName

    member __.GetStreamVersion(streamName: string) =
        connection
        |> Sql.existingConnection
        |> SqlClient.getStreamVersion streamName

    member __.GetCategory(streamName: string) =
        connection
        |> Sql.existingConnection
        |> SqlLib.getCategory streamName
        ||> Sql.executeRowAsync

    member __.DeleteMessage(id: System.Guid) =
        connection
        |> Sql.existingConnection
        |> SqlLib.deleteMessage id
        |> Sql.executeNonQueryAsync
