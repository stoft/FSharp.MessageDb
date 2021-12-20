open System
open System.Threading.Tasks

open FSharp.Control.Tasks.NonAffine

open NBomber
open NBomber.Contracts
open NBomber.FSharp
open FSharp.MessageDb
open FsToolkit.ErrorHandling

[<EntryPoint>]
let main argv =

    let cnxString =
        DbConnectionString.create
            { dbHost = "localhost"
              dbUsername = "admin"
              dbPassword = "secret"
              dbPort = 11111
              dbName = "message_store"
              dbSchema = "message_store" }
        |> DbConnectionString.toString
    let store = StatelessClient(cnxString)

    let msgBody =
        [| 1..200 |]
        |> Seq.map (fun _ -> string <| Random().Next(100))
        |> String.concat ""
        |> sprintf "\"%s\""

    let msg counter : UnrecordedMessage =
        { data = msgBody
          eventType = $"eventType-%d{counter}"
          id = Guid.NewGuid()
          metadata = None }

    let minutesToRun = 1
    let roughAvgPerSecond = 490

    let streamId incremented =
        incremented % (minutesToRun * 60 * roughAvgPerSecond / 10)

    let eventTypes =
        [ 1..50 ]
        |> Seq.ofList
        |> Feed.createCircular "ets"

    let writeMessage =
        Step.create (
            "writeMessage",
            feed = eventTypes,
            timeout = seconds 2,
            execute =
                fun context ->
                    let sid = $"load-%d{streamId context.InvocationCount}"
                    task {
                        do!
                            store.WriteMessage(sid, msg context.FeedItem)
                            |> Task.map ignore

                        return Response.ok ()
                    }
        )

    let readMessage =
        Step.create (
            "readMessage",
            (fun context ->
                task {
                    do!
                        store.GetCategoryMessages(
                            "loadTest",
                            context.InvocationCount * 10
                            |> int64
                            |> GlobalPosition,
                            Limited 30
                        )
                        |> Task.map ignore
                    return Response.ok ()
                })
        )

    let writeScenario =
        Scenario.create "writeMessage" [ writeMessage ]
        |> Scenario.withLoadSimulations [ KeepConstant(copies = 20, during = minutes minutesToRun) ]
        |> Scenario.withoutWarmUp

    let readScenario =
        Scenario.create "readMessage" [ readMessage ]
        |> Scenario.withLoadSimulations [ KeepConstant(copies = 5, during = minutes minutesToRun) ]
        |> Scenario.withoutWarmUp

    NBomberRunner.registerScenarios [ writeScenario ]
    //   readScenario ]
    |> NBomberRunner.run
    |> ignore

    0 // return an integer exit code
