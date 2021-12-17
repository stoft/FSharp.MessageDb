namespace FSharp.MessageDb.CategoryConsumer

open System.Threading.Tasks
open FsToolkit.ErrorHandling
open Npgsql.FSharp
open FSharp.MessageDb
open Serilog

module ConsumerLib =
    let delayTask (delayTimeSeconds: int) =
        let randomJitter = (System.Random().Next(delayTimeSeconds / 10)) //DevSkim: ignore DS148264

        let delayTime = delayTimeSeconds + randomJitter

        Task.Delay(System.TimeSpan.FromSeconds(float delayTime))
        |> Task.ofUnit

    let readBatch
        (client: StatelessClient)
        (batchSize: BatchSize)
        (categoryName: string)
        (handler: RecordedMessage -> Task<unit>)
        (consumerGroupMember: int)
        (consumerGroupSize: int)
        (fromPosition: GlobalPosition)
        : Task<Result<RecordedMessage option, exn>> =

        let handler': RecordedMessage -> Task<Result<unit, exn>> =
            handler >> TaskResult.ofTask

        taskResult {
            let! msgs =
                client.GetCategoryMessages(
                    categoryName,
                    position = fromPosition,
                    batchSize = batchSize,
                    consumerGroupMember = int64 consumerGroupMember,
                    consumerGroupSize = int64 consumerGroupSize
                )

            let! _ = List.traverseTaskResultM handler' msgs

            return List.tryLast msgs
        }

    let rec readContinuously
        (reader: GlobalPosition -> Task<Result<RecordedMessage option, exn>>)
        (savePos: GlobalPosition -> Task<unit>)
        (fromPosition: GlobalPosition)
        : Task<'a> =
        reader fromPosition
        |> Task.bind (function
            | Ok (Some msg) ->
                savePos msg.globalPosition
                |> Task.bind (fun () -> readContinuously reader savePos (GlobalPosition.addTo msg.globalPosition 1L))
            | Ok None ->
                (delayTask 5)
                |> Task.bind (fun () -> readContinuously reader savePos fromPosition)
            | Error e -> raise e)

    let rec doTryGetAdvisoryLockForStreamNames connection (streamNames: string list) : Task<string option> =
        match streamNames with
        | [] -> Task.singleton None
        | streamName :: tail ->
            SqlClient.doTryGetAdvisoryLockOnStreamName streamName (Sql.existingConnection connection)
            |> Task.bind (function
                | true -> Task.singleton (Some streamName)
                | false -> doTryGetAdvisoryLockForStreamNames connection tail)

    module LastReadPosition =
        let serialize (gp: GlobalPosition) : string =
            sprintf "\"%d\"" (GlobalPosition.toInt64 gp)

        let deserialize (s: string) : GlobalPosition =
            s.Trim('"')
            |> System.Int64.Parse
            |> GlobalPosition

        let writeLastReadPosition (client: StatelessClient) streamName (gp: GlobalPosition) : Task<unit> =
            let message: UnrecordedMessage =
                { data = serialize gp
                  eventType = "LastReadPosition"
                  id = System.Guid.NewGuid()
                  metadata = None }
            client.WriteMessage(streamName, message, Any)
            |> Task.map (function
                | Ok _ -> ()
                | Error (WrongExpectedVersion errMsg) -> failwith errMsg)

        let getLastReadPosition (client: StatelessClient) streamName : Task<GlobalPosition> =
            client.GetLastMessage(streamName)
            |> Task.map (function
                | Some msg -> deserialize msg.data
                | None -> GlobalPosition 0L)

module CompetingConsumer =
    let rec private doTryGetAdvisoryLockWithRetry
        connection
        retryCount
        (streamNames: string list)
        : Task<int * string> =
        let delayTime =
            if retryCount > 0 then
                printfn "locked out, sleeping"
                Log.Logger.Debug("Locked out, sleeping.")
                50
            else
                0

        let convertToGroupMember gp = streamNames |> List.findIndex ((=) gp)

        (ConsumerLib.delayTask delayTime)
        |> Task.bind (fun () ->
            ConsumerLib.doTryGetAdvisoryLockForStreamNames connection streamNames
            |> Task.bind (function
                | Some streamName ->
                    (convertToGroupMember streamName, streamName)
                    |> Task.singleton
                | None -> doTryGetAdvisoryLockWithRetry connection (retryCount + 1) streamNames))

    let internal getExclusiveLock (categoryName: string) connection (consumerGroupSize: int) =
        let streamName consumerGroupMember =
            StreamName.ofCategoryName categoryName $"%d{consumerGroupMember}"

        let streamNames = [ 1..consumerGroupSize ] |> List.map streamName

        doTryGetAdvisoryLockWithRetry connection 0 streamNames

    let ofConnectionString
        (connectionString: string)
        (logger: ILogger)
        (consumerName: string)
        (categoryName: string)
        (handler: RecordedMessage -> Task<unit>)
        (fromPosition: GlobalPosition)
        (consumerGroupSize: int)
        =
        let savePos = fun _ -> Task.singleton ()

        taskResult {
            let client = StatelessClient(connectionString)

            use connection = new Npgsql.NpgsqlConnection(connectionString)

            let! (groupMember, streamName) = getExclusiveLock consumerName connection consumerGroupSize

            return!
                ConsumerLib.readContinuously
                    (ConsumerLib.readBatch client (Limited 20) categoryName handler groupMember consumerGroupSize)
                    savePos
                    fromPosition
        }

module ExclusiveConsumer =

    /// Convenience function, this is equal to a competing consumer with a group size of 1
    let ofConnectionString
        (logger: ILogger)
        (connectionString: string)
        (consumerName: string)
        (categoryName: string)
        (handler: RecordedMessage -> Task<unit>)
        (fromPosition: GlobalPosition)
        =
        CompetingConsumer.ofConnectionString connectionString logger consumerName categoryName handler fromPosition 1

module PersistentConsumer =
    let ofConnectionString
        (connectionString: string)
        (logger: ILogger)
        (consumerName: string)
        (categoryName: string)
        (handler: RecordedMessage -> Task<unit>)
        (consumerGroupSize: int)
        =
        let savePos client sn =
            ConsumerLib.LastReadPosition.writeLastReadPosition client sn

        taskResult {
            let client = StatelessClient(connectionString)

            use connection = new Npgsql.NpgsqlConnection(connectionString)

            let! (groupMember, streamName) =
                CompetingConsumer.getExclusiveLock consumerName connection consumerGroupSize

            let! fromPosition =
                ConsumerLib.LastReadPosition.getLastReadPosition client streamName
                |> Task.map (fun gp -> GlobalPosition.addTo gp 1L)

            return!
                ConsumerLib.readContinuously
                    (ConsumerLib.readBatch client (Limited 20) categoryName handler groupMember consumerGroupSize)
                    (savePos client streamName)
                    fromPosition
        }
