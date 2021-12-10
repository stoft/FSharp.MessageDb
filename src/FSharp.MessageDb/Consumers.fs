namespace FSharp.MessageDb

open System.Threading.Tasks
open FsToolkit.ErrorHandling
open Npgsql.FSharp

module CompetingConsumer =
    let internal configureConsumerGroup
        (client: StatelessClient)
        (categoryName: string)
        (consumerGroupSize: int)
        : Task<GlobalPosition list> =

        let streamName consumerGroupMember =
            StreamName.ofCategoryName categoryName $"%d{consumerGroupMember}"

        let writeConsumerGroupMemberConfig (groupMember: int) =
            let lockMessage: UnrecordedMessage =
                { data = "{}"
                  eventType = "ConsumerConfigured"
                  id = System.Guid.NewGuid()
                  metadata = None }

            client.WriteMessage(streamName groupMember, lockMessage, NoStream)

        let getConfig consumerGroupMember : Task<GlobalPosition option> =
            client.TryStreamHead(streamName consumerGroupMember)
            |> Task.map (Option.map (fun msg -> GlobalPosition msg.globalPosition))

        let listOfGPs =
            [ 1 .. consumerGroupSize ]
            |> List.traverseTaskResultA writeConsumerGroupMemberConfig
            |> TaskResult.bindError
                (fun _ ->
                    [ 1 .. consumerGroupSize ]
                    |> List.traverseTaskResultM (
                        getConfig
                        >> Task.bind
                            (function
                            | None -> TaskResult.error (Failure "There should always be some value.")
                            | Some v -> TaskResult.ok v)
                    ))

        listOfGPs
        |> Task.map
            (function
            | Ok v -> v
            | Error e -> raise e)

    let internal doTryGetAdvisoryLock connection (key: int64) : Task<bool> =
        Sql.existingConnection connection
        |> Sql.func "pg_try_advisory_lock"
        |> Sql.parameters [ "key", Sql.int64 key ]
        |> Sql.executeAsync (fun read -> read.bool "pg_try_advisory_lock")
        |> Task.map List.head

    let rec internal doTryGetAdvisoryLockForGPs connection (gps: GlobalPosition list) : Task<GlobalPosition option> =
        match gps with
        | [] -> Task.singleton None
        | (GlobalPosition key) as gp :: tail ->
            doTryGetAdvisoryLock connection key
            |> Task.bind
                (function
                | true -> Task.singleton (Some gp)
                | false -> doTryGetAdvisoryLockForGPs connection tail)

    let rec internal doTryGetAdvisoryLockWithRetry connection retryCount (gps: GlobalPosition list) : Task<int> =
        if retryCount > 0 then
            Task
                .Delay(System.TimeSpan.FromSeconds(50.0 + (float <| System.Random().Next(10)))) //DevSkim: ignore DS148264
                .RunSynchronously()

        let convertToGroupMember gp = gps |> List.findIndex ((=) gp) |> (+) 1

        doTryGetAdvisoryLockForGPs connection gps
        |> Task.bind
            (function
            | Some gp -> convertToGroupMember gp |> Task.singleton
            | None -> doTryGetAdvisoryLockWithRetry connection (retryCount + 1) gps)

    let internal getExclusiveLock (name: string) client connection (consumerGroupSize: int) =
        let streamName = $"%s{name}-%d{consumerGroupSize}"

        configureConsumerGroup client streamName (int consumerGroupSize)
        |> Task.bind (fun gpList -> doTryGetAdvisoryLockWithRetry connection 0 gpList)

    let rec readContinuously
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

            return List.last msgs
        }
        |> Task.bind
            (function
            | Ok _msg ->
                readContinuously
                    client
                    batchSize
                    categoryName
                    handler
                    fromPosition
                    consumerGroupMember
                    consumerGroupSize
            | Error e -> raise e)

    let ofConnectionString
        (consumerName: string)
        (categoryName: string)
        (handler: RecordedMessage -> Task<unit>)
        (fromPosition: int64)
        (connectionString: string)
        (consumerGroupSize: int)
        =
        taskResult {
            let client = StatelessClient(connectionString)

            use connection =
                new Npgsql.NpgsqlConnection(connectionString)

            let! groupMember = getExclusiveLock consumerName client connection consumerGroupSize

            return!
                readContinuously client (Limited 20L) categoryName handler fromPosition groupMember consumerGroupSize

        }

module ExclusiveConsumer =

    /// Convenience function, this is equal to a competing consumer with a group size of 1
    let ofConnectionString a b c d e _ =
        CompetingConsumer.ofConnectionString a b c d e 1
