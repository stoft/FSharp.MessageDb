open Expecto
// open Serilog

[<EntryPoint>]
let main argv =

    // Log.Logger <-
    //     LoggerConfiguration()
    //         // .Destructure.FSharpTypes()
    //         .WriteTo
    //         .Console()
    //         .CreateLogger()

    runTestsInAssemblyWithCLIArgs (Seq.singleton <| Colours 256) argv
