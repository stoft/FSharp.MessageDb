namespace FSharp.MessageDb.CategoryConsumer

open System.Threading.Tasks
open FsToolkit.ErrorHandling
open Npgsql.FSharp
open FSharp.MessageDb
open Microsoft.Extensions.Logging

module ConsumerLib =
    let delayTask (delayTimeSeconds: int) =
        let randomJitter =
            (System.Random().Next(delayTimeSeconds / 10)) //DevSkim: ignore DS148264

        let delayTime = delayTimeSeconds + randomJitter

        Task.Delay(System.TimeSpan.FromSeconds(float delayTime))
        |> Task.ofUnit

    let readBatch
        (client: StatelessClient)
        (batchSize: BatchSize)
        (categoryName: string)
        (handler: RecordedMessage -> Task<unit>)
        (fromPosition: int64)
        (consumerGroupMember: int)
        (consumerGroupSize: int)
        =
        let handler': RecordedMessage -> Task<Result<unit, exn>> = handler >> TaskResult.ofTask

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
        (client: StatelessClient)
        (batchSize: BatchSize)
        (categoryName: string)
        (handler: RecordedMessage -> Task<unit>)
        (fromPosition: int64)
        (consumerGroupMember: int)
        (consumerGroupSize: int)
        =

        let rc fromPos' =
            readContinuously client batchSize categoryName handler fromPos' consumerGroupMember consumerGroupSize

        readBatch client batchSize categoryName handler fromPosition consumerGroupMember consumerGroupSize
        |> Task.bind
            (function
            | Ok (Some msg) -> rc (msg.globalPosition + 1L)
            | Ok None ->
                (delayTask 5)
                |> Task.bind (fun () -> rc fromPosition)
            | Error e -> raise e)

module CompetingConsumer =
    let rec doTryGetAdvisoryLockForStreamNames connection (streamNames: string list) : Task<string option> =
        match streamNames with
        | [] -> Task.singleton None
        | streamName :: tail ->
            SqlClient.doTryGetAdvisoryLockOnStreamName streamName (Sql.existingConnection connection)
            |> Task.bind
                (function
                | true -> Task.singleton (Some streamName)
                | false -> doTryGetAdvisoryLockForStreamNames connection tail)

    let rec private doTryGetAdvisoryLockWithRetry connection retryCount (streamNames: string list) : Task<int> =
        let delayTime = if retryCount > 0 then 50 else 0

        let convertToGroupMember gp = streamNames |> List.findIndex ((=) gp)

        (ConsumerLib.delayTask delayTime)
        |> Task.bind
            (fun () ->
                doTryGetAdvisoryLockForStreamNames connection streamNames
                |> Task.bind
                    (function
                    | Some sn -> convertToGroupMember sn |> Task.singleton
                    | None -> doTryGetAdvisoryLockWithRetry connection (retryCount + 1) streamNames))

    let internal getExclusiveLock (categoryName: string) connection (consumerGroupSize: int) =
        let streamName consumerGroupMember =
            StreamName.ofCategoryName categoryName $"%d{consumerGroupMember}"

        let streamNames =
            [ 1 .. consumerGroupSize ] |> List.map streamName

        doTryGetAdvisoryLockWithRetry connection 0 streamNames


    let ofConnectionString
        (connectionString: string)
        (consumerName: string)
        (categoryName: string)
        (handler: RecordedMessage -> Task<unit>)
        (fromPosition: int64)
        (consumerGroupSize: int)
        =
        taskResult {
            let client = StatelessClient(connectionString)

            use connection =
                new Npgsql.NpgsqlConnection(connectionString)

            let! groupMember = getExclusiveLock consumerName connection consumerGroupSize

            return!
                ConsumerLib.readContinuously
                    client
                    (Limited 20)
                    categoryName
                    handler
                    fromPosition
                    groupMember
                    consumerGroupSize
        }

module ExclusiveConsumer =

    /// Convenience function, this is equal to a competing consumer with a group size of 1
    let ofConnectionString
        (connectionString: string)
        // (logger: ILogger)
        (consumerName: string)
        (categoryName: string)
        (handler: RecordedMessage -> Task<unit>)
        (fromPosition: int64)
        =
        CompetingConsumer.ofConnectionString connectionString consumerName categoryName handler fromPosition 1
