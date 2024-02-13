/*
 *
 * Copyright (c) Sari Sultan (sarisultan@ieee.org | sari.sultan@mail.utoronto.ca)
 *
 * Part of the artifact evaluation code for Sultan et al.'s EuroSys'24 paper titled:
 * TTLs Matter: Efficient Cache Sizing with TTL-Aware Miss Ratio Curves and Working Set Sizes
 *
 * If you have any questions or want to report a bug please feel free to contact me anytime : )
 * If you want to optimize the code, please make a pull request and I will make sure to check it out, thanks.
 *
 */


using System.Diagnostics;
using TTLsMatter.Benchmarks;
using TTLsMatter.Common.Hashing;
using TTLsMatter.Datasets.Common.Types;
using TTLsMatter.MRC.Common;

namespace TTLsMatter;

class Program
{
    const string WSS_RESULTS_DIR = "WSS-AE-2024";
    const string MRC_RESULTS_DIR = "MRCs-AE-2024";
    const string MRC_PLOTS_DATA_MAD_DIR = "MRCs-AE-2024-plots-data-mad";
    const string MRC_PLOTS_DATA_MAE_DIR = "MRCs-AE-2024-plots-data-mae";
    const string MRC_PLOTS_DATA_THROUGHPUT_DIR = "MRCs-AE-2024-plots-data-throughput";

    /// <summary>
    /// The main program for the Artifact Evaluation of the paper
    /// TTLs Matter - EuroSys'24 - Sari Sultan et al. 
    ///
    /// The evaluation section of the paper contains 5 figures (i.e., [Fig 14 - Fig 18]).
    /// After running this code (few days later) you will have a directory called "Figures" that includes
    /// all the figures. We have spent significant time to make sure all you need to do is to run the program once (nothing else).
    /// 
    /// </summary>
    static void Main()
    {
        var startTime = DateTime.Now;
        try
        {
            Console.WriteLine($"{DateTime.Now}. Main() Started: TTL's Matter - EuroSys'24 - Artifact evaluation.");

            /*Generate the Working Set Size (WSS) Results - Figures 14 & 15 & 16*/
            GenerateWSSResults();
            GenerateWssPlots();

            /*Generate the Miss Ratio Curve (MRC) results - Figures 17A, 17B, & 18*/
            GenerateMRCResults();
            GenerateMRCPlots();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"{DateTime.Now}. Exception occured in the main program. EX: [{ex.ToString()}]");
        }
        finally
        {
            var endTime = DateTime.Now;
            var elapsedTime = endTime - startTime;
            Console.WriteLine($"{DateTime.Now}. Main() Finished: TTL's Matter - EuroSys'24 - Artifact evaluation.\n" +
                              $"Time required to complete the evaluation: {elapsedTime.Days} days {elapsedTime.Hours} hours {elapsedTime.Minutes} minutes {elapsedTime.Seconds} seconds");
        }
    }


    static void GenerateWSSResults()
    {
        Console.WriteLine($"{DateTime.Now}: [+] Starting {nameof(WSSBenchmarks.WssExactNoTTL)}");
        WSSBenchmarks.WssExactNoTTL(WSS_RESULTS_DIR, 1 * 60 * 60);
        Console.WriteLine($"{DateTime.Now}: [+] Finished {nameof(WSSBenchmarks.WssExactNoTTL)}");

        Console.WriteLine($"{DateTime.Now}: [+] Starting {nameof(WSSBenchmarks.WssExactTTL)}");
        WSSBenchmarks.WssExactTTL(WSS_RESULTS_DIR, 1 * 60 * 60);
        Console.WriteLine($"{DateTime.Now}: [+] Finished {nameof(WSSBenchmarks.WssExactTTL)}");

        Console.WriteLine($"{DateTime.Now}: [+] Starting {nameof(WSSBenchmarks.WssHLLNoTTL)}");
        WSSBenchmarks.WssHLLNoTTL(WSS_RESULTS_DIR, 1 * 60 * 60, false);
        Console.WriteLine($"{DateTime.Now}: [+] Finished {nameof(WSSBenchmarks.WssHLLNoTTL)}");

        Console.WriteLine($"{DateTime.Now}: [+] Starting {nameof(WSSBenchmarks.WssHLLTTL)}");
        WSSBenchmarks.WssHLLTTL(WSS_RESULTS_DIR, 1 * 60 * 60, true);
        Console.WriteLine($"{DateTime.Now}: [+] Finished {nameof(WSSBenchmarks.WssHLLTTL)}");

        Console.WriteLine($"{DateTime.Now}: [+] Starting {nameof(WSSBenchmarks.ResultsAnalysis)}");
        WSSBenchmarks.ResultsAnalysis(WSS_RESULTS_DIR);
        Console.WriteLine($"{DateTime.Now}: [+] Finished {nameof(WSSBenchmarks.ResultsAnalysis)}");
    }

    /// <summary>
    /// This function generate the data required for the MRC results in the
    /// evaluation section of the accepted paper (TTLs Matter - EuroSys'24).
    /// In particular:
    ///     [+] Figure 17 - MRC accuracy results.
    ///     [+] Figure 18 - MRC throughput results. 
    ///
    /// This code takes a lot of time (>1 day)
    /// </summary>
    static void GenerateMRCResults()
    {
        var datasetsToTest = new List<DataSetType>()
        {
            DataSetType.FilteredTwitter,
        };

        /*Olken-TTL. This is an exact algorithm. Run it by itself because it uses a lot of memory.*/
        MrcBenchmark.Run(MRC_RESULTS_DIR, datasetsToTest, new() { MrcGenerationAlgoType.OlkenTtl });

        /*Olken-NoTTL. This is an exact algorithm. Run it by itself because it uses a lot of memory.*/
        MrcBenchmark.Run(MRC_RESULTS_DIR, datasetsToTest, new() { MrcGenerationAlgoType.OlkenNoTtl });

        /*Approximate TTL and No TTL together because they use less memory*/
        var algorithmsToTest = new List<MrcGenerationAlgoType>()
        {
            /*TTL FR-Shards*/
            MrcGenerationAlgoType.ShardsFixedRateTtlPoint1,
            MrcGenerationAlgoType.ShardsFixedRateTtlPointZero1,
            MrcGenerationAlgoType.ShardsFixedRateTtlPointZeroZero1,
            MrcGenerationAlgoType.ShardsAdjFixedRateTtlPoint1,
            MrcGenerationAlgoType.ShardsAdjFixedRateTtlPointZero1,
            MrcGenerationAlgoType.ShardsAdjFixedRateTtlPointZeroZero1,

            /*TTL FS-Shards*/
            MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl1K,
            MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl2K,
            MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl4K,
            MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl8K,
            MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl16K,
            MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl32K,
            MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl64K,

            /*No-TTL FR-Shards*/
            MrcGenerationAlgoType.ShardsFixedRateNoTtlPoint1,
            MrcGenerationAlgoType.ShardsFixedRateNoTtlPointZero1,
            MrcGenerationAlgoType.ShardsFixedRateNoTtlPointZeroZero1,
            MrcGenerationAlgoType.ShardsAdjFixedRateNoTtlPoint1,
            MrcGenerationAlgoType.ShardsAdjFixedRateNoTtlPointZero1,
            MrcGenerationAlgoType.ShardsAdjFixedRateNoTtlPointZeroZero1,

            /*No-TTL FS-Shards*/
            MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl1K,
            MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl2K,
            MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl4K,
            MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl8K,
            MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl16K,
            MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl32K,
            MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl64K,

            /*CS++*/
            MrcGenerationAlgoType.CounterstacksPlusPlusTtlFbs50CountersP12T8HiFi,
            MrcGenerationAlgoType.CounterstacksPlusPlusTtlFbs50CountersP12T8LoFi,

            /*No-TTL Counterstacks*/
            MrcGenerationAlgoType.CounterStacksHiFiP12NoTtl,
            MrcGenerationAlgoType.CounterStacksLoFiP12NoTtl,
        };
        MrcBenchmark.Run(MRC_RESULTS_DIR, datasetsToTest, algorithmsToTest);


        /*Generate the plotting csvs for the deviation between nottl and ttl mrcs*/
        MrcBenchmark.PreparePlottingResults($"{MRC_PLOTS_DATA_MAD_DIR}"
            , MRC_RESULTS_DIR
            , datasetsToTest,
            new List<MrcGenerationAlgoType>()
            {
                MrcGenerationAlgoType.OlkenNoTtl,
                MrcGenerationAlgoType.ShardsFixedRateNoTtlPoint1,
                MrcGenerationAlgoType.ShardsAdjFixedRateNoTtlPoint1,
                MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl1K,
                MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl64K,
            },
            MrcGenerationAlgoType.OlkenTtl);


        /*Generate the plotting csvs for the MAE between approximate ttl and exact ttl mrcs*/
        MrcBenchmark.PreparePlottingResults($"{MRC_PLOTS_DATA_MAE_DIR}"
            , MRC_RESULTS_DIR
            , datasetsToTest,
            new List<MrcGenerationAlgoType>()
            {
                /*TTL FR-Shards*/
                MrcGenerationAlgoType.ShardsFixedRateTtlPoint1,
                MrcGenerationAlgoType.ShardsFixedRateTtlPointZero1,
                MrcGenerationAlgoType.ShardsFixedRateTtlPointZeroZero1,
                MrcGenerationAlgoType.ShardsAdjFixedRateTtlPoint1,
                MrcGenerationAlgoType.ShardsAdjFixedRateTtlPointZero1,
                MrcGenerationAlgoType.ShardsAdjFixedRateTtlPointZeroZero1,

                /*TTL FS-Shards*/
                MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl1K,
                MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl2K,
                MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl4K,
                MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl8K,
                MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl16K,
                MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl32K,
                MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl64K,

                /*CS++*/
                MrcGenerationAlgoType.CounterstacksPlusPlusTtlFbs50CountersP12T8HiFi,
                MrcGenerationAlgoType.CounterstacksPlusPlusTtlFbs50CountersP12T8LoFi,
            },
            MrcGenerationAlgoType.OlkenTtl);


        /*Generate the plotting csvs for the throughput of tested algorithms*/
        MrcBenchmark.PreparePlottingResults($"{MRC_PLOTS_DATA_THROUGHPUT_DIR}"
            , MRC_RESULTS_DIR
            , datasetsToTest,
            new List<MrcGenerationAlgoType>()
            {
                MrcGenerationAlgoType.OlkenNoTtl,

                /*No-TTL FR-Shards*/
                MrcGenerationAlgoType.ShardsFixedRateNoTtlPoint1,
                MrcGenerationAlgoType.ShardsAdjFixedRateNoTtlPoint1,

                /*No-TTL FS-Shards*/
                MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl1K,
                MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl64K,

                /*No-TTL Counterstacks*/
                MrcGenerationAlgoType.CounterStacksHiFiP12NoTtl,
                MrcGenerationAlgoType.CounterStacksLoFiP12NoTtl,

                MrcGenerationAlgoType.OlkenTtl,

                /*TTL FR-Shards*/
                MrcGenerationAlgoType.ShardsFixedRateTtlPoint1,
                MrcGenerationAlgoType.ShardsFixedRateTtlPointZero1,
                MrcGenerationAlgoType.ShardsFixedRateTtlPointZeroZero1,
                MrcGenerationAlgoType.ShardsAdjFixedRateTtlPoint1,
                MrcGenerationAlgoType.ShardsAdjFixedRateTtlPointZero1,
                MrcGenerationAlgoType.ShardsAdjFixedRateTtlPointZeroZero1,

                /*TTL FS-Shards*/
                MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl1K,
                MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl2K,
                MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl4K,
                MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl8K,
                MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl16K,
                MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl32K,
                MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl64K,

                /*CS++*/
                MrcGenerationAlgoType.CounterstacksPlusPlusTtlFbs50CountersP12T8HiFi,
                MrcGenerationAlgoType.CounterstacksPlusPlusTtlFbs50CountersP12T8LoFi,
            },
            MrcGenerationAlgoType.OlkenTtl);
    }


    #region PDF plots generation

    #region WSS

    static void GenerateWssPlots()
    {
        //Figure 14. Relative memory savings across workloads
        WSS_FIG14_RelativeMemorySavings("Figures/Figure14-WSS-RelativeMemorySavings", WSS_RESULTS_DIR);

        //Figure 15. Sensitivity of the precision parameter, 𝑏, for HLLTTL
        WSS_FIG15_ParameterSensitivity("Figures/Figure15-HLL-TTL-ParameterSensitivity", WSS_RESULTS_DIR);

        //Figure 16. Throughput of HLL-TTL for different precisions
        WSS_FIG16_HLLTTL_Throughput("Figures/Figure16-HLL-TTL-Throughput", WSS_RESULTS_DIR);
    }

    static void WSS_FIG14_RelativeMemorySavings(string output_dir, string input_dir)
    {
        var name = $"{nameof(WSS_FIG14_RelativeMemorySavings)}";
        Console.WriteLine($"{DateTime.Now}: started {name}()");
        if (!Directory.Exists(input_dir))
        {
            Console.WriteLine($"Error in {name}, the input directory does not exist [{input_dir}]");
            return;
        }

        Directory.CreateDirectory(output_dir);

        //copy the required CSV file to the output directory
        var inputCsvFilePath = Path.Combine(input_dir, "Gap-Exact-WSS-nottl-vs-ttl.csv");
        var outputCsvFilePath = Path.Combine(output_dir, "Gap-Exact-WSS-nottl-vs-ttl.csv");
        if (!File.Exists(inputCsvFilePath))
        {
            Console.WriteLine($"Error in {name}, the input file does not exist [{inputCsvFilePath}]");
            return;
        }


        File.Copy(inputCsvFilePath, outputCsvFilePath, true);

        var gnuplot_script = @"reset
set datafile separator ','
set terminal pdf enhanced size 6cm, 2cm
set output 'FIGURE14-wss-savings-exact-ttl-vs-nottl.pdf'

set tmargin 0.25
set bmargin 1.35
set lmargin 2.75
set rmargin 0.5

set ylabel ""Savings (%)"" font ',7' offset 4.5,-.5
set xlabel ""Twitter Workload ID"" font ',7' offset 0,1.45
set xtics font ',7' rotate by 90 right offset 0.0,0.45  nomirror
set ytics font ',7' offset 1,0 nomirror
set ytics 0,20,100
set yrange [0:100]
set boxwidth 0.5
set style fill solid
set grid y
set xtics scale 0
set ytics scale 0
#set border 2
#set arrow from graph 0, first 99.96 to graph 1, first 99.96 nohead lc rgb ""red"" dt 2 lw 3
#set label ""99.96%"" at graph 0.9, graph 1.03 font ',6' tc rgb 'red'

plot 'Gap-Exact-WSS-nottl-vs-ttl.csv' using 5:xtic(1) with boxes notitle";

        var gnuplot_file_path = Path.Combine(output_dir, "fig14.plt");
        File.WriteAllText(gnuplot_file_path, gnuplot_script);

        Console.WriteLine($"[AE {DateTime.Now}] GnuPlot Script File Outputted:\n" +
                          $"\tFILE PATH: [{gnuplot_file_path}]\n" +
                          $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(gnuplot_file_path)}]");


        //run the gnuplot command 
        var gnuplotOutput = RunGnuPlotScript(output_dir, gnuplot_file_path);

        Thread.Sleep(2 * 1000);
        var output_pdf_file_path = Path.Combine(output_dir, "FIGURE14-wss-savings-exact-ttl-vs-nottl.pdf");
        if (File.Exists(output_pdf_file_path))
        {
            Console.WriteLine($"[AE {DateTime.Now}] GnuPlot PDF File Outputted:\n" +
                              $"\tFILE PATH: [{output_pdf_file_path}]\n" +
                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(output_pdf_file_path)}]");
        }
        else
        {
            Console.WriteLine(
                $"Error in {name}, failed to generate the pdf file [{output_pdf_file_path}]. Gnuplot output for debugging: {gnuplotOutput}");
            return;
        }

        Console.WriteLine($"{DateTime.Now}: finished {name}()");
    }

    static void WSS_FIG15_ParameterSensitivity(string output_dir, string input_dir)
    {
        var name = $"{nameof(WSS_FIG15_ParameterSensitivity)}";
        Console.WriteLine($"{DateTime.Now}: started {name}()");
        if (!Directory.Exists(input_dir))
        {
            Console.WriteLine($"Error in {name}, the input directory does not exist [{input_dir}]");
            return;
        }

        Directory.CreateDirectory(output_dir);

        //copy the required CSV file to the output directory
        var inputCsvFilePath = Path.Combine(input_dir, "wss-hll-ttl-error.csv");
        var outputCsvFilePath = Path.Combine(output_dir, "wss-hll-ttl-error.csv");
        if (!File.Exists(inputCsvFilePath))
        {
            Console.WriteLine($"Error in {name}, the input file does not exist [{inputCsvFilePath}]");
            return;
        }


        File.Copy(inputCsvFilePath, outputCsvFilePath, true);

        var gnuplot_script = @"reset 
set terminal pdf enhanced  size 4cm,2cm
set datafile separator ','
set output ""FIGURE15-wss-hll-ttl-error.pdf""

set ylabel ""ARE (%)"" offset 4.25,0 font "",7""
set xlabel ""HLL Precision (b)"" offset 0,1.75 font "",7""
set xrange [7.5:17]
set xtics font ',7' offset 0,0.75
set xtics scale 0

set xtics 8,1,16 nomirror
set yrange [0:10]
set ytics font "",7"" offset 1,0
set ytics add ('1' 1) font "",6.5""
#set ytics add (' ' 0)
#set ytics ( ""1"" 0.01, ""5"" 0.05, ""10"" 0.10, ""15"" 0.15, ""20"" 0.20, ""25"" 0.25)
#set xlabel ""Algorithm"" offset 1,2.5 font "",8""
set offsets 0.5, 0.5,0, 0
set errorbars 2.0
set style fill empty
#set logscaley 2
set bmargin 1.0
set tmargin 0.25
set lmargin 2.0
set rmargin 0.25

set offsets 1,1,0,0

set macros 

set ytics nomirror

set key top right font ',7'  box
set key spacing 1.25

set grid ytics
#set arrow from graph 0, first 99 to graph 1, first 99 nohead lc rgb ""red"" dt 1
#set label ""99\%"" at graph 1, first 99 right offset char -1,0 font "",8""


#x:25th:min:max:75th
plot 'wss-hll-ttl-error.csv' using 1:10:3:4:11 with candlesticks notitle,\
'' using 1:6 with points title ""Mean"" pt 5 ps 0.3,\
'' using 1:5 with points title ""Median"" pt 9 ps 0.3,\
'' using 1:12 with points title ""{/Symbol= s}"" pt 7 ps 0.2
";

        var gnuplot_file_path = Path.Combine(output_dir, "fig15.plt");
        File.WriteAllText(gnuplot_file_path, gnuplot_script);

        Console.WriteLine($"[AE {DateTime.Now}] GnuPlot Script File Outputted:\n" +
                          $"\tFILE PATH: [{gnuplot_file_path}]\n" +
                          $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(gnuplot_file_path)}]");


        //run the gnuplot command 
        var gnuplotOutput = RunGnuPlotScript(output_dir, gnuplot_file_path);

        Thread.Sleep(2 * 1000);
        var output_pdf_file_path = Path.Combine(output_dir, "FIGURE15-wss-hll-ttl-error.pdf");
        if (File.Exists(output_pdf_file_path))
        {
            Console.WriteLine($"[AE {DateTime.Now}] GnuPlot PDF File Outputted:\n" +
                              $"\tFILE PATH: [{output_pdf_file_path}]\n" +
                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(output_pdf_file_path)}]");
        }
        else
        {
            Console.WriteLine(
                $"Error in {name}, failed to generate the pdf file [{output_pdf_file_path}]. Gnuplot output for debugging: {gnuplotOutput}");
            return;
        }

        Console.WriteLine($"{DateTime.Now}: finished {name}()");
    }

    static void WSS_FIG16_HLLTTL_Throughput(string output_dir, string input_dir)
    {
        var name = $"{nameof(WSS_FIG16_HLLTTL_Throughput)}";
        Console.WriteLine($"{DateTime.Now}: started {name}()");
        if (!Directory.Exists(input_dir))
        {
            Console.WriteLine($"Error in {name}, the input directory does not exist [{input_dir}]");
            return;
        }

        Directory.CreateDirectory(output_dir);

        //copy the required CSV file to the output directory
        var inputCsvFilePath = Path.Combine(input_dir, "wss-hll-ttl-throughput-with-io.csv");
        var outputCsvFilePath = Path.Combine(output_dir, "wss-hll-ttl-throughput-with-io.csv");
        if (!File.Exists(inputCsvFilePath))
        {
            Console.WriteLine($"Error in {name}, the input file does not exist [{inputCsvFilePath}]");
            return;
        }


        File.Copy(inputCsvFilePath, outputCsvFilePath, true);

        var gnuplot_script = @"reset 
set terminal pdf enhanced  size 4cm,2cm
set datafile separator ','
set output ""FIGURE16-wss-hll-ttl-throughput-with-io.pdf""

set ylabel ""Mil. Access/Sec"" offset 4.2,0 font "",8""
set xlabel ""HLL Precision (b)"" offset 0,1.75 font "",8""
set xrange [7.5:16.25]
set xtics 8,1,16 nomirror

set ytics font "",7"" offset 1,0
set yrange [0:*]
set offsets 0.25, 0.5,0, 0
set errorbars 2.0
set style fill empty
#set logscaley 2
set bmargin 1.0
set tmargin 0.25
set lmargin 3
set rmargin 0.25

set macros 

set xtics font ',7' offset 0,0.75
set ytics nomirror

set key bottom left font ',7' box
#set key at screen 0.985,0.96 top right font ',6' box
set key spacing 1.0

set grid ytics
#set arrow from graph 0, first 99 to graph 1, first 99 nohead lc rgb ""red"" dt 1
#set label ""99\%"" at graph 1, first 99 right offset char -1,0 font "",8""


#x:25th:min:max:75th
plot 'wss-hll-ttl-throughput-with-io.csv' using 1:($10/1000000):($3/1000000):($4/1000000):($11/1000000) with candlesticks notitle,\
'' using 1:($6/1000000) with points title ""Mean"" pt 5 ps 0.3,\
'' using 1:($5/1000000) with points title ""Median"" pt 9 ps 0.3
";

        var gnuplot_file_path = Path.Combine(output_dir, "fig16.plt");
        File.WriteAllText(gnuplot_file_path, gnuplot_script);

        Console.WriteLine($"[AE {DateTime.Now}] GnuPlot Script File Outputted:\n" +
                          $"\tFILE PATH: [{gnuplot_file_path}]\n" +
                          $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(gnuplot_file_path)}]");


        //run the gnuplot command 
        var gnuplotOutput = RunGnuPlotScript(output_dir, gnuplot_file_path);

        Thread.Sleep(2 * 1000);
        var output_pdf_file_path = Path.Combine(output_dir, "FIGURE16-wss-hll-ttl-throughput-with-io.pdf");
        if (File.Exists(output_pdf_file_path))
        {
            Console.WriteLine($"[AE {DateTime.Now}] GnuPlot PDF File Outputted:\n" +
                              $"\tFILE PATH: [{output_pdf_file_path}]\n" +
                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(output_pdf_file_path)}]");
        }
        else
        {
            Console.WriteLine(
                $"Error in {name}, failed to generate the pdf file [{output_pdf_file_path}]. Gnuplot output for debugging: {gnuplotOutput}");
            return;
        }

        Console.WriteLine($"{DateTime.Now}: finished {name}()");
    }

    #endregion WSS

    #region MRC

    static void GenerateMRCPlots()
    {
        //Figure 17A. Deviation between mrc-nottl and mrc-ttl
        MRC_FIG17A_deviation("Figures/Figure17A-MRC-Deviation", MRC_PLOTS_DATA_MAD_DIR);

        //Figure 17A. Deviation between mrc-nottl and mrc-ttl
        MRC_FIG17B_accuracy("Figures/Figure17B-MRC-Deviation", MRC_PLOTS_DATA_MAE_DIR);

        //Figure 18. Throughput 
        MRC_FIG18_throughput("Figures/Figure18-MRC-Throughput", MRC_PLOTS_DATA_THROUGHPUT_DIR);
    }


    static void MRC_FIG17A_deviation(string output_dir, string input_dir)
    {
        var name = $"{nameof(MRC_FIG17A_deviation)}";
        Console.WriteLine($"{DateTime.Now}: started {name}()");
        if (!Directory.Exists(input_dir))
        {
            Console.WriteLine($"Error in {name}, the input directory does not exist [{input_dir}]");
            return;
        }

        Directory.CreateDirectory(output_dir);

        //copy the required CSV file to the output directory
        var inputCsvFilePath = Path.Combine(input_dir, "runningAvg-mae-avg.csv");
        var outputCsvFilePath = Path.Combine(output_dir, "runningAvg-mae-avg.csv");
        if (!File.Exists(inputCsvFilePath))
        {
            Console.WriteLine($"Error in {name}, the input file does not exist [{inputCsvFilePath}]");
            return;
        }

        File.Copy(inputCsvFilePath, outputCsvFilePath, true);

        //xtics
        var inputxticsFilePath = Path.Combine(input_dir, "runningAvg-mae-avg.xtics");
        var outputxticsFilePath = Path.Combine(output_dir, "runningAvg-mae-avg.xtics");
        if (!File.Exists(inputxticsFilePath))
        {
            Console.WriteLine($"Error in {name}, the input file does not exist [{inputxticsFilePath}]");
            return;
        }

        File.Copy(inputxticsFilePath, outputxticsFilePath, true);


        var gnuplot_script = @"reset 
set terminal pdf enhanced size 1.5cm,3.0cm
set datafile separator ','
set output ""FIGURE17A-runningAvg-mad-avg.pdf""

set offsets 0.5, 0.5,0, 0

set bmargin 3.5
set tmargin 0.5
set lmargin 2.5
set rmargin 0.25

set errorbars 2
set style fill empty

set macros 
XTICS=system(""cat runningAvg-mae-avg.xtics"")

set ytics font "",6"" offset 1,0
#set logscale y 2
#set ytics 1,2,64
#set ytics add ('.5' 0.5, '{/=5.5 .125}' .125, '{/=5.5 .002}' 0.002)
#set yrange [0.002:64]
set ylabel ""MAD (%)"" font ',7' offset 4,0 
set ytics nomirror


set xtics rotate font ',6.5' @XTICS offset -0.1,0.5 #by 45 right 
set xtics nomirror scale 0


#set key at screen 0.99,0.94 top right font ',6' box
set grid y


# Add vertical lines for grouping
#set arrow from 5.5, graph 0 to 5.5, graph 1 nohead linetype -1 dt 1 linewidth 1.0

# Add labels for groups
#set label ""MAD (%)"" at 3, graph 1.05 center font "",7""
#set label ""MAE (%)"" at 15, graph 1.05 center font "",7""

set label 1 ""(a)"" at 0, graph -0.6 center font '{/:Bold},10'
unset key

plot 'runningAvg-mae-avg.csv' using 1:($11*100):($6*100):($7*100):($12*100) with candlesticks notitle,\
'' using 1:($4*100) with points title ""Mean"" pt 5 ps 0.3,\
'' using 1:($5*100) with points title ""Median"" pt 9 ps 0.3
";

        var gnuplot_file_path = Path.Combine(output_dir, "fig17-A.plt");
        File.WriteAllText(gnuplot_file_path, gnuplot_script);

        Console.WriteLine($"[AE {DateTime.Now}] GnuPlot Script File Outputted:\n" +
                          $"\tFILE PATH: [{gnuplot_file_path}]\n" +
                          $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(gnuplot_file_path)}]");


        //run the gnuplot command 
        var gnuplotOutput = RunGnuPlotScript(output_dir, gnuplot_file_path);

        Thread.Sleep(2 * 1000);
        var output_pdf_file_path = Path.Combine(output_dir, "FIGURE17A-runningAvg-mad-avg.pdf");
        if (File.Exists(output_pdf_file_path))
        {
            Console.WriteLine($"[AE {DateTime.Now}] GnuPlot PDF File Outputted:\n" +
                              $"\tFILE PATH: [{output_pdf_file_path}]\n" +
                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(output_pdf_file_path)}]");
        }
        else
        {
            Console.WriteLine(
                $"Error in {name}, failed to generate the pdf file [{output_pdf_file_path}]. Gnuplot output for debugging: {gnuplotOutput}");
            return;
        }

        Console.WriteLine($"{DateTime.Now}: finished {name}()");
    }

    static void MRC_FIG17B_accuracy(string output_dir, string input_dir)
    {
        var name = $"{nameof(MRC_FIG17B_accuracy)}";
        Console.WriteLine($"{DateTime.Now}: started {name}()");
        if (!Directory.Exists(input_dir))
        {
            Console.WriteLine($"Error in {name}, the input directory does not exist [{input_dir}]");
            return;
        }

        Directory.CreateDirectory(output_dir);

        //copy the required CSV file to the output directory
        var inputCsvFilePath = Path.Combine(input_dir, "runningAvg-mae-avg.csv");
        var outputCsvFilePath = Path.Combine(output_dir, "runningAvg-mae-avg.csv");
        if (!File.Exists(inputCsvFilePath))
        {
            Console.WriteLine($"Error in {name}, the input file does not exist [{inputCsvFilePath}]");
            return;
        }

        File.Copy(inputCsvFilePath, outputCsvFilePath, true);

        //xtics
        var inputxticsFilePath = Path.Combine(input_dir, "runningAvg-mae-avg.xtics");
        var outputxticsFilePath = Path.Combine(output_dir, "runningAvg-mae-avg.xtics");
        if (!File.Exists(inputxticsFilePath))
        {
            Console.WriteLine($"Error in {name}, the input file does not exist [{inputxticsFilePath}]");
            return;
        }

        File.Copy(inputxticsFilePath, outputxticsFilePath, true);


        var gnuplot_script = @"reset 
set terminal pdf enhanced size 4.5cm,3cm
set datafile separator ','
set output ""FIGURE17B-runningAvg-mae-avg.pdf""

set offsets 0.5, 0.5,0, 0

set bmargin 3.8
set tmargin 0.25
set lmargin 2.25
set rmargin 0.25

set errorbars 2
set style fill empty

set macros 
XTICS=system(""cat runningAvg-mae-avg.xtics"")

set ytics font "",5.5"" offset 1,0
#set logscale y 2
#set ytics 1,2,64
#set ytics add ('.5' 0.5, '{/=5.5 .125}' .125, '{/=5.5 .002}' 0.002)
#set yrange [0.002:64]
set ylabel ""MAE (%)"" font ',7' offset 4,0
set ytics nomirror
set yrange [0:9]
set ytics add ('{/=4 0}' 0, '0.5' 0.5)

set xtics rotate font ',6.5' @XTICS offset -0.1,0.45 #by 45 right 
set xtics nomirror scale 0


set key at screen 0.99,0.94 top right font ',6' box
set grid y


# Add vertical lines for grouping
#set arrow from 5.5, graph 0 to 5.5, graph 1 nohead linetype -1 dt 1 linewidth 1.0

# Add labels for groups
#set label ""MAD (%)"" at 3, graph 1.05 center font "",7""
#set label ""MAE (%)"" at 15, graph 1.05 center font "",7""

set label 1 ""(b)"" at 1.1, graph -0.65 center font '{/:Bold},10'

plot 'runningAvg-mae-avg.csv' using 1:($11*100):($6*100):($7*100):($12*100) with candlesticks notitle,\
'' using 1:($4*100) with points title ""Mean"" pt 5 ps 0.3,\
'' using 1:($5*100) with points title ""Median"" pt 9 ps 0.3

";

        var gnuplot_file_path = Path.Combine(output_dir, "fig17-B.plt");
        File.WriteAllText(gnuplot_file_path, gnuplot_script);

        Console.WriteLine($"[AE {DateTime.Now}] GnuPlot Script File Outputted:\n" +
                          $"\tFILE PATH: [{gnuplot_file_path}]\n" +
                          $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(gnuplot_file_path)}]");


        //run the gnuplot command 
        var gnuplotOutput = RunGnuPlotScript(output_dir, gnuplot_file_path);

        Thread.Sleep(2 * 1000);
        var output_pdf_file_path = Path.Combine(output_dir, "FIGURE17B-runningAvg-mae-avg.pdf");
        if (File.Exists(output_pdf_file_path))
        {
            Console.WriteLine($"[AE {DateTime.Now}] GnuPlot PDF File Outputted:\n" +
                              $"\tFILE PATH: [{output_pdf_file_path}]\n" +
                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(output_pdf_file_path)}]");
        }
        else
        {
            Console.WriteLine(
                $"Error in {name}, failed to generate the pdf file [{output_pdf_file_path}]. Gnuplot output for debugging: {gnuplotOutput}");
            return;
        }

        Console.WriteLine($"{DateTime.Now}: finished {name}()");
    }

    static void MRC_FIG18_throughput(string output_dir, string input_dir)
    {
        var name = $"{nameof(MRC_FIG18_throughput)}";
        Console.WriteLine($"{DateTime.Now}: started {name}()");
        if (!Directory.Exists(input_dir))
        {
            Console.WriteLine($"Error in {name}, the input directory does not exist [{input_dir}]");
            return;
        }

        Directory.CreateDirectory(output_dir);

        //copy the required CSV file to the output directory
        var inputCsvFilePath = Path.Combine(input_dir, "fbs-throughput-withIO-avg.csv");
        var outputCsvFilePath = Path.Combine(output_dir, "fbs-throughput-withIO-avg.csv");
        if (!File.Exists(inputCsvFilePath))
        {
            Console.WriteLine($"Error in {name}, the input file does not exist [{inputCsvFilePath}]");
            return;
        }

        File.Copy(inputCsvFilePath, outputCsvFilePath, true);

        //xtics
        var inputxticsFilePath = Path.Combine(input_dir, "fbs-mae-throughput-withIO.xtics");
        var outputxticsFilePath = Path.Combine(output_dir, "fbs-mae-throughput-withIO.xtics");
        if (!File.Exists(inputxticsFilePath))
        {
            Console.WriteLine($"Error in {name}, the input file does not exist [{inputxticsFilePath}]");
            return;
        }

        File.Copy(inputxticsFilePath, outputxticsFilePath, true);


        var gnuplot_script = @"reset 
set terminal pdf enhanced size 5.5cm,3cm
set datafile separator ','
set output ""FIGURE18-fbs-throughput-withio-avg.pdf""

set ylabel ""Throughput [M. accesses/sec]"" offset 5.5,-2 font "",7""
#set yrange [0.00001:.25]
set logscale y 2
#set format y ""10^{%L}"" 
set ytics font "",7"" offset 1,0
set ytics 0,2,256
set ytics add ('.5' 0.5, ' ' .25)
#set ytics font "",8"" ( ""0.01"" 0.01, ""0.05"" 0.05, ""0.10"" 0.10, ""0.15"" 0.15, ""0.20"" 0.20, ""0.25"" 0.25)
#set xlabel ""Algorithm"" offset 1,2.5 font "",8""
set offsets 0.5, 0.5,0, 0
set errorbars 1.0
set style fill empty

set bmargin 3.75
set tmargin 0.25
set lmargin 3.0
set rmargin 0.25

set macros 
XTICS=system(""cat fbs-mae-throughput-withIO.xtics"")

set xtics rotate font ',6.5' @XTICS offset 0,0.5 #by 45 right
set xtics nomirror
set ytics nomirror

set key bottom right font ',7' box 
set grid
#set title ""Uniform Block Sizes (with IO)"" offset 0,-0.75 font "",9""

plot 'fbs-throughput-withIO-avg.csv' using 1:($11/1000000):($6/1000000):($7/1000000):($12/1000000) with candlesticks notitle,\
'' using ($4/1000000) with points title ""Mean"" pt 5 ps 0.2,\
'' using ($5/1000000) with points title ""Median"" pt 9 ps 0.3

";

        var gnuplot_file_path = Path.Combine(output_dir, "fig17-B.plt");
        File.WriteAllText(gnuplot_file_path, gnuplot_script);

        Console.WriteLine($"[AE {DateTime.Now}] GnuPlot Script File Outputted:\n" +
                          $"\tFILE PATH: [{gnuplot_file_path}]\n" +
                          $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(gnuplot_file_path)}]");


        //run the gnuplot command 
        var gnuplotOutput = RunGnuPlotScript(output_dir, gnuplot_file_path);

        Thread.Sleep(2 * 1000);
        var output_pdf_file_path = Path.Combine(output_dir, "FIGURE18-fbs-throughput-withio-avg.pdf");
        if (File.Exists(output_pdf_file_path))
        {
            Console.WriteLine($"[AE {DateTime.Now}] GnuPlot PDF File Outputted:\n" +
                              $"\tFILE PATH: [{output_pdf_file_path}]\n" +
                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(output_pdf_file_path)}]");
        }
        else
        {
            Console.WriteLine(
                $"Error in {name}, failed to generate the pdf file [{output_pdf_file_path}]. Gnuplot output for debugging: {gnuplotOutput}");
            return;
        }

        Console.WriteLine($"{DateTime.Now}: finished {name}()");
    }

    #endregion MRC

    #region helper

    public static string RunGnuPlotScript(string workingDir, string gnuplotfilename)
    {
        var abdWorkingDirPath = Path.GetFullPath(workingDir);
        var absScriptPath = Path.GetFullPath(gnuplotfilename);
        var process = new Process
        {
            StartInfo = new ProcessStartInfo()
            {
                WorkingDirectory = abdWorkingDirPath,
                FileName = "/usr/bin/gnuplot",
                Arguments = absScriptPath,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            }
        };
        process.Start();

        var output = "STDOUT";
        while (!process.StandardOutput.EndOfStream)
            output += process.StandardOutput.ReadLine() + "\n";

        output += "STDERR";
        while (!process.StandardError.EndOfStream)
            output += process.StandardError.ReadLine() + "\n";

        return output;
    }

    #endregion helper

    #endregion PDF plots generation
}