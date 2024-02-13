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

//#define debug_limit_nr_accesses_mrc //used to limit the number of accesses for debugging

using System.Diagnostics;
using System.Text;
using LinqStatistics;
using MathNet.Numerics.Statistics;
using TTLsMatter.Common.GCHelpers;
using TTLsMatter.Common.Hashing;
using TTLsMatter.Common.Statistics;
using TTLsMatter.Datasets.Common.Types;
using TTLsMatter.Datasets.Factory;
using TTLsMatter.MRC.Common;
using TTLsMatter.MRC.Factory;
using TTLsMatter.MRC.Tools;

namespace TTLsMatter.Benchmarks;

public static class MrcBenchmark
{
#if debug_limit_nr_accesses_mrc
    private const long DEBUG_NUM_ACCESSES = 1000 * 1000 * 1000;
    private const int BatchSize = 1000 * 1000;
#else
    private const int BatchSize = 10 * 1000 * 1000;
#endif

    public static void Run(
        string mainOutputDir
        , List<DataSetType> datasets
        , List<MrcGenerationAlgoType> algorithms)
    {
        Console.WriteLine($"{DateTime.Now}: STARTED: {nameof(MrcBenchmark)}-{nameof(Run)}()");

        Console.WriteLine(Report(datasets, algorithms));
        var appDomainIoMutex = new object();

        long aggregateRequestsPerDatasets = 0;
        foreach (var dataset in datasets)
        {
            List<string> traceFiles = DatasetFactory.GetTraceFiles(dataset);

            foreach (var trace in traceFiles)
            {
                var reader = DatasetFactory.GetReader(dataset, trace, null, null, null, 1);
                aggregateRequestsPerDatasets += reader.GetNumberOfRequests();
            }
        }

        foreach (var dataset in datasets)
        {
            var traceFiles = DatasetFactory.GetTraceFiles(dataset);

            long aggregateProgress = 0;
            var aggregateTimer = Stopwatch.StartNew();

            foreach (var traceFile in traceFiles)
            {
                GCHelper.COLLECTMAX();

                var traceName = Path.GetFileNameWithoutExtension(traceFile);
                Console.WriteLine($"[+] {DateTime.Now}: Started processing Dataset: {dataset} Trace: {traceName}");

                /*Statistics stopwatches*/
                var ioTimer = new Stopwatch();
                var totalTimer = new Stopwatch(); /*Total timer for all instances*/


                /* Create list of algorithms to test...
                 * We need to maintain a processing time timer per algorithm
                 */

                var instances = new List<AlgorithmBenchmarkInstance>();
                foreach (var algorithm in algorithms)
                {
                    var algoDir = Path.Combine(mainOutputDir, dataset.ToString(), algorithm.ToString());
                    var fbsDir = Path.Combine(algoDir, "fbs");

                    var fbsfilePath = Path.Combine(fbsDir, $"{traceName}.csv");

                    if (!File.Exists(fbsfilePath))
                    {
                        instances.Add(new AlgorithmBenchmarkInstance(algorithm,
                            MrcAlgorithmFactory.GetAlgorithm(algorithm), dataset, traceFile, traceName, ioTimer,
                            new()));
                        Console.WriteLine(
                            $"Added {algorithm} to instances because the CSVs doesnt exit. [{fbsfilePath}] ");
                    }
                    else
                    {
                        Console.WriteLine($"Skipping {fbsfilePath} because it already exists.");
                    }
                }

                if (!instances.Any())
                {
                    Console.WriteLine($"No instances to run");
                    var rtemp =
                        DatasetFactory.GetReader(dataset, traceFile, appDomainIoMutex, ioTimer, totalTimer, 1);
                    
                    /*this is to make the ETA numbering accurate*/
                    aggregateRequestsPerDatasets -= rtemp.GetNumberOfRequests(); 
                    continue;
                }

                /*Get binary reader*/
                using var reader = DatasetFactory.GetReader(dataset, traceFile, appDomainIoMutex, ioTimer, totalTimer,
                    BatchSize);

                var totalNumberOfRequests = reader.GetNumberOfRequests();
                var reqs = reader.Requests;
                long totalProcessed = 0L;
                while (true)
                {
                    var items = reader.GetBatch();
                    if (items == -1) break;

                    /*Here the total timer should be running and the io timer should be stopped*/
                    if (!totalTimer.IsRunning || ioTimer.IsRunning)
                    {
                        throw new Exception("Total timer should not be running. OR ioTimer should be stopped.");
                    }

                    foreach (var instance in instances)
                    {
                        instance.ProcessingTimer.Start();
                        instance.Instance.AddRequests(reqs, 0, items);
                        instance.ProcessingTimer.Stop();
                    }

                    totalProcessed += items;
                    aggregateProgress += items;
                    long remainingAggregateRequests = aggregateRequestsPerDatasets - aggregateProgress;
                    var aggregateThroughput = aggregateProgress / aggregateTimer.Elapsed.TotalSeconds;
                    var remainingTimeSeconds = remainingAggregateRequests / aggregateThroughput;
                    var remainingTimeSpan = TimeSpan.FromSeconds(remainingTimeSeconds);
                    Console.Write(
                        $"\r{DateTime.Now} MRC DST[{dataset}]-[{traceName}] " +
                        $"L ({(double)totalProcessed / totalNumberOfRequests * 100.0:f2}%) [{totalProcessed:n0}/{totalNumberOfRequests:n0}] " +
                        $"G ({(double)aggregateProgress / aggregateRequestsPerDatasets * 100.0:f2}%) [{aggregateProgress:n0}/{aggregateRequestsPerDatasets:n0}] " +
                        $"ETA {remainingTimeSpan.Days}D:{remainingTimeSpan.Hours}H:{remainingTimeSpan.Minutes}M:{remainingTimeSpan.Seconds}S ({aggregateThroughput:n0} rps)");

#if debug_limit_nr_accesses_mrc
                        if (totalProcessed >= DEBUG_NUM_ACCESSES)
                            break;
#endif
                }

                Console.WriteLine();

                /*OUTPUT MRCs and Statistics*/
                foreach (var instance in instances)
                {
                    var algoDir = Path.Combine(mainOutputDir, dataset.ToString(), instance.Config.ToString());

                    /*I will output the statistics per file, with a .stat extension*/
                    var sb = new StringBuilder();
                    sb.AppendLine(
                        "TraceName, Dataset, Algorithm, NumberOfLinesProcessed, IOTime(s), " +
                        "ProcessingTimeWithoutIO(s), TotalTime(s), ProcessingSpeed(r/s), OverallSpeedWithIO(r/s)");
                    var totalTimeSeconds =
                        ioTimer.Elapsed.TotalSeconds + instance.ProcessingTimer.Elapsed.TotalSeconds;
                    sb.AppendLine(
                        $"{traceName}, {instance.DataSetType.ToString()}, {instance.Config}, {totalProcessed}, {ioTimer.Elapsed.TotalSeconds:f6}, " +
                        $"{instance.ProcessingTimer.Elapsed.TotalSeconds:f6}, {totalTimeSeconds:f6}, {totalProcessed / instance.ProcessingTimer.Elapsed.TotalSeconds:f6}, {totalProcessed / totalTimeSeconds:f6}");

                    var fbsDir = Path.Combine(algoDir, "fbs");
                    Directory.CreateDirectory(fbsDir);
                    
                    {
                        var fbsFilePath = Path.Combine(fbsDir, $"{traceName}.csv");
                        var fbsStatFilePath = Path.Combine(fbsDir, $"{traceName}.stat");
                        var fbsMrc = instance.Instance.GetMrc_FixedBlockSize();
                        if (!string.IsNullOrEmpty(fbsMrc))
                        {
                            File.WriteAllText(fbsFilePath, fbsMrc);
                            Console.WriteLine($"[AE {DateTime.Now}] MRC File Outputted:\n" +
                                              $"\tFILE PATH: [{fbsFilePath}]\n" +
                                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(fbsFilePath)}]");

                            File.WriteAllText(fbsStatFilePath, sb.ToString());
                            Console.WriteLine($"[AE {DateTime.Now}] STAT File Outputted/Updated:\n" +
                                              $"\tFILE PATH: [{fbsStatFilePath}]\n" +
                                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(fbsStatFilePath)}]");
                        }
                    }

                    {
                        var runningavgDir = Path.Combine(algoDir, "runningAvg");
                        Directory.CreateDirectory(runningavgDir);
                        var runningAvgFilePath = Path.Combine(runningavgDir, $"{traceName}.csv");
                        var runningStatAvgFilePath = Path.Combine(runningavgDir, $"{traceName}.stat");
                        var runningAvgMrc = instance.Instance.GetMrc_VariableBlockSize_RunningAverage();
                        if (!string.IsNullOrEmpty(runningAvgMrc))
                        {
                            File.WriteAllText(runningAvgFilePath, runningAvgMrc);
                            Console.WriteLine($"[AE {DateTime.Now}] MRC File Outputted:\n" +
                                              $"\tFILE PATH: [{runningAvgFilePath}]\n" +
                                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(runningAvgFilePath)}]");

                            File.WriteAllText(runningStatAvgFilePath, sb.ToString());
                            Console.WriteLine($"[AE {DateTime.Now}] STAT File Outputted/Updated:\n" +
                                              $"\tFILE PATH: [{runningStatAvgFilePath}]\n" +
                                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(runningStatAvgFilePath)}]");
                        }
                    }
                    
                }//!foreach (var instance in instances)

                GCHelper.COLLECTMAX();
            } //!foreach (var traceFile in traceFiles)
        }

        Console.WriteLine($"{DateTime.Now}: FINISHED: {nameof(MrcBenchmark)}-{nameof(Run)}()");
    }


    #region helpers

    private static string Report( List<DataSetType> datasets
        , List<MrcGenerationAlgoType> algorithms)
    {
        var report = new StringBuilder();
        report.AppendLine($"[+] Traces to benchmark:");
        foreach (var dataset in datasets)
        {
            List<string> traceFiles = DatasetFactory.GetTraceFiles(dataset);
            foreach (var file in traceFiles)
            {
                var traceName = Path.GetFileNameWithoutExtension(file);
                report.AppendLine($"\t{dataset.ToString()}: {traceName}");
            }
        }

        report.AppendLine($"[+] Algorithms to benchmark:");
        foreach (var algorithm in algorithms)
        {
            report.AppendLine($"\t{algorithm.ToString()}");
        }

        return report.ToString();
    }

    #endregion helpers


    #region internal classes

    private class AlgorithmBenchmarkInstance
    {
        public AlgorithmBenchmarkInstance(MrcGenerationAlgoType config, IMrcGenerationAlgorithm instance,
            DataSetType dataSetType, string traceFilePath, string traceName, Stopwatch ioTimer,
            Stopwatch processingTimer)
        {
            Config = config;
            Instance = instance;
            DataSetType = dataSetType;
            TraceFilePath = traceFilePath;
            TraceName = traceName;
            IoTimer = ioTimer;
            ProcessingTimer = processingTimer;
        }

        public MrcGenerationAlgoType Config { get; set; }
        public IMrcGenerationAlgorithm Instance { get; set; }
        public DataSetType DataSetType { get; set; }
        public string TraceFilePath { get; set; }
        public string TraceName { get; set; }
        public Stopwatch IoTimer { get; set; }
        public Stopwatch ProcessingTimer { get; set; }

        public long ProcessingSpeedWithoutIo(long totalNumRequests)
        {
            return (long)(totalNumRequests / ProcessingTimer.Elapsed.TotalSeconds);
        }
    }

    internal class MrcAlgoResultEntry
    {
        public enum Configtype
        {
            fbs,
            runningAvg
        }

        public Configtype ct;

        public MrcAlgoResultEntry(string traceName)
        {
            TraceName = traceName;
        }

        public MrcGenerationAlgoType AlgoType { get; set; }
        public DataSetType DataSetType { get; set; }
        public string TraceName { get; set; }
        public double Mae { get; set; }
        public double NumberOfPointsForComputingTheMae { get; set; }
        public long ThroughputWithoutIO { get; set; }
        public long ThroughputWithIO { get; set; }

        public override string ToString()
        {
            return
                $"{nameof(Configtype)}: {ct},, {nameof(AlgoType)}: {AlgoType}, {nameof(DataSetType)}: {DataSetType}, {nameof(TraceName)}: {TraceName}, {nameof(Mae)}: {Mae}, {nameof(NumberOfPointsForComputingTheMae)}: {NumberOfPointsForComputingTheMae}, {nameof(ThroughputWithoutIO)}: {ThroughputWithoutIO}, {nameof(ThroughputWithIO)}: {ThroughputWithIO}";
        }
    }

    #endregion internal classes


    #region preparing results for plotting

    public static void PreparePlottingResults(
        string outputDir
        , string inputDir
        , List<DataSetType> datasets,
        List<MrcGenerationAlgoType> algorithms,
        MrcGenerationAlgoType refAlgo)
    {
        Console.WriteLine($"{DateTime.Now} - Starting {nameof(PreparePlottingResults)}()");
        
        var dataSetNames = Directory.GetDirectories(inputDir)
            .Select(x => Path.GetFileNameWithoutExtension(x))
            .ToList();

        List<MrcAlgoResultEntry> results = new List<MrcAlgoResultEntry>(1000);
        object resultsMutex = new object();


        foreach (var dsname in dataSetNames)
        {
            Enum.TryParse<DataSetType>(dsname, out var dsEnum);
            if (!datasets.Contains(dsEnum))
            {
                Console.WriteLine($"WARNING: Skipping dataset {dsname} as it's not in the list");
                continue;
            }

            var refDir = Path.Combine(inputDir, dsname, refAlgo.ToString(), "fbs");

            DataSetType dataset = Enum.Parse<DataSetType>(dsname);
            var traceNames = Directory.GetFiles(refDir, "*.csv")
                .Select(x => Path.GetFileNameWithoutExtension(x))
                .ToList();

            foreach (var traceName in traceNames)
            {
                var refalgoDir = Path.Combine(inputDir, dataset.ToString(), refAlgo.ToString());

                var reffbsDir = Path.Combine(refalgoDir, MrcAlgoResultEntry.Configtype.fbs.ToString());
                var reffbsFilePath = Path.Combine(reffbsDir, $"{traceName}.csv");
                string reffbsMrc = null;
                if (File.Exists(reffbsFilePath))
                    reffbsMrc = File.ReadAllText(reffbsFilePath);

                var refavgDir = Path.Combine(refalgoDir, MrcAlgoResultEntry.Configtype.runningAvg.ToString());
                var refavgFilePath = Path.Combine(refavgDir, $"{traceName}.csv");
                string refrunningAvgMrc = null;
                if (File.Exists(refavgFilePath))
                    refrunningAvgMrc = File.ReadAllText(refavgFilePath);


                Parallel.ForEach(algorithms, algo =>
                {
                    var algoDir = Path.Combine(inputDir, dataset.ToString(), algo.ToString());

                    if (!string.IsNullOrEmpty(reffbsMrc))
                    {
                        var fbsDir = Path.Combine(algoDir, MrcAlgoResultEntry.Configtype.fbs.ToString());
                        var fbsFilePath = Path.Combine(fbsDir, $"{traceName}.csv");
                        var fbsStatFilePath = Path.Combine(fbsDir, $"{traceName}.stat");

                        if (File.Exists(fbsFilePath) && File.Exists(fbsStatFilePath))
                        {
                            var fbsMrc = File.ReadAllText(fbsFilePath);
                            var mae = MaeCalculator.ComputeMae(reffbsMrc, fbsMrc);

                            if (!double.IsNaN(mae.Item1))
                            {
                                /*Make stat processing a function*/
                                var fbsStat = File.ReadAllLines(fbsStatFilePath).Skip(1).First();
                                // "[0]TraceName, [1]Dataset, [2]Algorithm, [3]NumberOfLinesProcessed, [4]IOTime(s), " +
                                //     "[5]ProcessingTimeWithoutIO(s), [6]TotalTime(s), [7]ProcessingSpeed(r/s), [8]OverallSpeedWithIO(r/s)
                                var parts = fbsStat.Split(',');
                                var throughputWithoutIo = (long)double.Parse(parts[7]);
                                var throughputWithIo = (long)double.Parse(parts[8]);
                                lock (resultsMutex)
                                {
                                    results.Add(new MrcAlgoResultEntry(traceName)
                                    {
                                        DataSetType = dataset,
                                        AlgoType = algo,
                                        Mae = mae.Item1,
                                        NumberOfPointsForComputingTheMae = mae.Item2,
                                        ThroughputWithIO = throughputWithIo,
                                        ThroughputWithoutIO = throughputWithoutIo,
                                        ct = MrcAlgoResultEntry.Configtype.fbs
                                    });
                                }
                            }
                        }
                    }

                    {
                        var runningavgDir = Path.Combine(algoDir, MrcAlgoResultEntry.Configtype.runningAvg.ToString());
                        var runningAvgFilePath = Path.Combine(runningavgDir, $"{traceName}.csv");
                        var runningStatAvgFilePath = Path.Combine(runningavgDir, $"{traceName}.stat");
                        if (File.Exists(runningAvgFilePath))
                        {
                            var runningAvgMrc = File.ReadAllText(runningAvgFilePath);
                            var mae = MaeCalculator.ComputeMae(refrunningAvgMrc, runningAvgMrc);

                            if (!double.IsNaN(mae.Item1))
                            {
                                /*Make stat processing a function*/
                                var fbsStat = File.ReadAllLines(runningStatAvgFilePath).Skip(1).First();
                                // "[0]TraceName, [1]Dataset, [2]Algorithm, [3]NumberOfLinesProcessed, [4]IOTime(s), " +
                                //     "[5]ProcessingTimeWithoutIO(s), [6]TotalTime(s), [7]ProcessingSpeed(r/s), [8]OverallSpeedWithIO(r/s)
                                var parts = fbsStat.Split(',');
                                var throughputWithoutIo = (long)double.Parse(parts[7]);
                                var throughputWithIo = (long)double.Parse(parts[8]);
                                lock (resultsMutex)
                                {
                                    results.Add(new MrcAlgoResultEntry(traceName)
                                    {
                                        DataSetType = dataset,
                                        AlgoType = algo,
                                        Mae = mae.Item1,
                                        NumberOfPointsForComputingTheMae = mae.Item2,
                                        ThroughputWithIO = throughputWithIo,
                                        ThroughputWithoutIO = throughputWithoutIo,
                                        ct = MrcAlgoResultEntry.Configtype.runningAvg
                                    });
                                }
                            }
                        }
                    }
                }); //!  foreach (var algo in algorithms)
            } //!foreach (var traceFile in traceFiles)
        } //!foreach (var dataset in datasets)


        /*Reporting*/
        Directory.CreateDirectory(outputDir);

        foreach (var ct in new List<MrcAlgoResultEntry.Configtype>()
                 {
                     MrcAlgoResultEntry.Configtype.fbs,
                     MrcAlgoResultEntry.Configtype.runningAvg
                 })
        {
            var reports = GetMaeThroughputAverageReports(results, ct, refAlgo);
            var t = ct.ToString();

            var maeReportAndXtics = GetXTicsAndNewReport(reports.Item1);
            File.WriteAllText(Path.Combine(outputDir, $"{t}-mae-avg.csv"), maeReportAndXtics.Item1);
            File.WriteAllText(Path.Combine(outputDir, $"{t}-mae-avg.xtics"), maeReportAndXtics.Item2);

            var tWithIoAndXtics = GetXTicsAndNewReport(reports.Item2);
            File.WriteAllText(Path.Combine(outputDir, $"{t}-throughput-withIO-avg.csv"), tWithIoAndXtics.Item1);
            File.WriteAllText(Path.Combine(outputDir, $"{t}-mae-throughput-withIO.xtics"), tWithIoAndXtics.Item2);

            var tWithoutIoAndXtics = GetXTicsAndNewReport(reports.Item3);
            File.WriteAllText(Path.Combine(outputDir, $"{t}-throughput-withoutIO-avg.csv"),
                tWithoutIoAndXtics.Item1);
            File.WriteAllText(Path.Combine(outputDir, $"{t}-mae-throughput-withoutIO.xtics"),
                tWithoutIoAndXtics.Item2);
        }

        Console.WriteLine($"{DateTime.Now} - Finished {nameof(PreparePlottingResults)}(). Output Dir: [{outputDir}]");
    }

    private static (string, string, string) GetMaeThroughputAverageReports(List<MrcAlgoResultEntry> results,
        MrcAlgoResultEntry.Configtype ct, MrcGenerationAlgoType refAlgo)
    {
        var groupedByAlgo = results
                .Where(x => x.ct == ct)
                .GroupBy(x => x.AlgoType)
                .OrderBy(x => (int)x.Key)
            ;

        var datasetsString = "";
        foreach (var e in Enumerable.DistinctBy(results, x => x.DataSetType))
            datasetsString += $"{e.DataSetType.ToString()}+";

        var maeAvgCsv = new StringBuilder();
        maeAvgCsv.AppendLine(
            "AlgorithmCode, AlgoName, NumberOfTraces, AverageMAE, MedianMAE, MinMAE, MaxMAE, " +
            "StdevMAE, 95CIMinMAE, 95CIMaxMAE, 25thPercentileMAE, 75thPercentileMAE,  Datasets," +
            " NumPoints>=0.2, NumPoints>=0.25, NumPoints>=0.3, NumPoints>=0.4,25th(oldCalculation),75th(oldCalculation)");

        var throughputWithIOAvgCsv = new StringBuilder();
        throughputWithIOAvgCsv.AppendLine(
            "AlgorithmCode, AlgoName, NumberOfTraces, AverageThroughput, Median, Min, Max, " +
            "StdevMAE, 95CIMinMAE, 95CIMaxMAE, 25thPercentileMAE, 75thPercentileMAE,  Datasets,25th(oldCalculation),75th(oldCalculation)");

        var throughputWithoutIOAvgCsv = new StringBuilder();
        throughputWithoutIOAvgCsv.AppendLine(
            "AlgorithmCode, AlgoName, NumberOfTraces, AverageThroughput, Median, Min, Max, " +
            "StdevMAE, 95CIMinMAE, 95CIMaxMAE, 25thPercentileMAE, 75thPercentileMAE,  Datasets,25th(oldCalculation),75th(oldCalculation)");

        foreach (var g in groupedByAlgo)
        {
            var algo = g.Key;
            var algoInt = (int)algo;
            var numberOfTraces = g.Count();
            var values = g
                .ToList();

            if (algo != refAlgo)
            {
                /*MAE*/
                var nans = values.Where(x => double.IsNaN(x.Mae));
                var maes = values
                    .Where(x => !double.IsNaN(x.Mae))
                    .Select(x => x.Mae)
                    .ToList()
                    .OrderBy(x => x)
                    .ToList();

                var average = maes.Average();
                var median = EnumerableStats.Median(maes);
                var min = maes.Min();
                var max = maes.Max();
                var stdev = maes.StandardDeviationP();
                var ci95 = ConfidenceIntervalHelper.Get95Confidence(maes);
                var ci95min = average - ci95;
                var ci95max = average + ci95;
                var twentyfifthPercentile = maes.Percentile(25);
                var seventyfifthPercentile = maes.Percentile(75);
                var twentyfifthPercentileOld = PercentileHelper.Percentile(maes, 0.25);
                var seventyfifthPercentileOld = PercentileHelper.Percentile(maes, 0.75);
                var numPointsLpoint2 = maes.Count(x => x >= 0.20);
                var numPointsLpoint25 = maes.Count(x => x >= 0.25);
                var numPointsLpoint3 = maes.Count(x => x >= 0.3);
                var numPointsLpoint4 = maes.Count(x => x >= 0.4);
                maeAvgCsv.AppendLine(
                    $"{algoInt}, {algo.ToString()}, {numberOfTraces}, {average:f6}, {median:f6}, {min:f6}, {max:f6}, " +
                    $"{stdev:f6}, {ci95min:f6}, {ci95max:f6}, {twentyfifthPercentile:f6}, {seventyfifthPercentile:f6}, {datasetsString}, " +
                    $"{numPointsLpoint2},{numPointsLpoint25},{numPointsLpoint3},{numPointsLpoint4},{twentyfifthPercentileOld:f6},{seventyfifthPercentileOld:f6}");
            } //!/*MAE*/

            {
                /*Throughput with IO*/
                var throughputs = values
                    .Where(x => !double.IsNaN(x.Mae))
                    .Select(x => x.ThroughputWithIO)
                    .ToList()
                    .OrderBy(x => x)
                    .Select(x => (double)x)
                    .ToList();

                var average = throughputs.Average();
                var median = throughputs.Median(x => x);
                var min = throughputs.Min();
                var max = throughputs.Max();
                var stdev = throughputs.StandardDeviationP();
                var ci95 = ConfidenceIntervalHelper.Get95Confidence(throughputs);
                var ci95min = average - ci95;
                var ci95max = average + ci95;
                var twentyfifthPercentile = throughputs.Percentile(25);
                var seventyfifthPercentile = throughputs.Percentile(75);
                var twentyfifthPercentileOld = PercentileHelper.Percentile(throughputs, 0.25);
                var seventyfifthPercentileOld = PercentileHelper.Percentile(throughputs, 0.75);
                throughputWithIOAvgCsv.AppendLine(
                    $"{algoInt}, {algo.ToString()}, {numberOfTraces}, {average:f6}, {median:f6}, {min:f6}, {max:f6}, " +
                    $"{stdev:f6}, {ci95min:f6}, {ci95max:f6}, {twentyfifthPercentile:f6}, {seventyfifthPercentile:f6}, {datasetsString},{twentyfifthPercentileOld:f6},{seventyfifthPercentileOld:f6}");
            } //!/*Throughput with IO*/

            {
                /*Throughput without IO*/
                var throughputs = values
                        .Where(x => !double.IsNaN(x.Mae))
                        .Select(x => x.ThroughputWithoutIO)
                        .ToList()
                        .OrderBy(x => x)
                        .Select(x => (double)x)
                        .ToList()
                    ;

                var average = throughputs.Average();
                var median = throughputs.Median(x => x);
                var min = throughputs.Min();
                var max = throughputs.Max();
                var stdev = throughputs.StandardDeviationP();
                var ci95 = ConfidenceIntervalHelper.Get95Confidence(throughputs);
                var ci95min = average - ci95;
                var ci95max = average + ci95;
                var twentyfifthPercentile = throughputs.Percentile(25);
                var seventyfifthPercentile = throughputs.Percentile(75);
                var twentyfifthPercentileOld = PercentileHelper.Percentile(throughputs, 0.25);
                var seventyfifthPercentileOld = PercentileHelper.Percentile(throughputs, 0.75);
                throughputWithoutIOAvgCsv.AppendLine(
                    $"{algoInt}, {algo.ToString()}, {numberOfTraces}, {average:f6}, {median:f6}, {min:f6}, {max:f6}, " +
                    $"{stdev:f6}, {ci95min:f6}, {ci95max:f6}, {twentyfifthPercentile:f6}, {seventyfifthPercentile:f6}, {datasetsString},{twentyfifthPercentileOld:f6},{seventyfifthPercentileOld:f6}");
            } //!/*Throughput without IO*/
        }

        return (maeAvgCsv.ToString(), throughputWithIOAvgCsv.ToString(), throughputWithoutIOAvgCsv.ToString());
    }

    private static (string, string) GetXTicsAndNewReport(string report)
    {
        var lines = report.Split(Environment.NewLine).Where(x => !string.IsNullOrEmpty(x)).ToArray();

        /*Assumes a csv with [0] algoInt [1] algoType*/
        var xtics = new StringBuilder();
        xtics.Append("( ");

        var newReport = new StringBuilder();
        newReport.AppendLine(lines[0]);
        for (var li = 1; li < lines.Length; li++)
        {
            var line = lines[li];

            try
            {
                var vals = line.Split(',');
                var algoIntCode = int.Parse(vals[0]);
                algoIntCode = li;
                var algoType = Enum.Parse<MrcGenerationAlgoType>(vals[1]);
                xtics.Append($"'{algoType.ToString()}' {li}, ");

                newReport.Append($"{algoIntCode}, {algoType.ToString()}, ");
                for (int j = 2; j < vals.Length; j++)
                {
                    newReport.Append($"{vals[j]}, ");
                }

                newReport.AppendLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error while parsing line[{li}] from the report.\nLine= {line}");
                throw;
            }
        }

        xtics.Append(")");
        return (ChangeAlgoNames(newReport.ToString()), ChangeAlgoNames(xtics.ToString()));
    }

    private static string ChangeAlgoNames(string input)
    {
        /*Extended Algos*/
        input = input.Replace("OlkenTtl", "Olken^{{/=4 ++}}");
        input = input.Replace("ShardsFixedRateTtl", "FR-SH^{{/=4 ++}}");
        input = input.Replace("ShardsAdjFixedSpaceTtl", "FS-SH_{adj}^{{/=4 ++}}");
        input = input.Replace("ShardsAdjFixedRateTtl", "FR-SH_{adj}^{{/=4 ++}}");
        input = input.Replace("CounterstacksPlusPlusTtlFbs", "CS^{{/=4 ++}}");
        input = input.Replace("50Counters", "C50");
        input = input.Replace("100Counters", "C100");
        input = input.Replace("200Counters", "C200");

        /*OLD ALGOS*/
        input = input.Replace("ShardsFixedRate", "FR-SH");
        input = input.Replace("ShardsAdjFixedRate", "FR-SH_{adj}");
        input = input.Replace("ShardsAdjFixedSpace", "FS-SH_{adj}");
        input = input.Replace("CounterStacks", "CS-");
        input = input.Replace("T8", "");

        /*PARAMS*/
        input = input.Replace("NoTtl", "");
        input = input.Replace("Ttl", "");

        input = input.Replace("Point1", "-.1");
        input = input.Replace("PointZero1", "-.01");
        input = input.Replace("PointZeroZero1", "-.001");

        input = input.Replace("MBP", "MB-b");
        input = input.Replace("LoFi", "LoFi");
        input = input.Replace("HiFi", "HiFi");

        input = input.Replace("Fbs", "");

        return input;
    }

    #endregion preparing results for plotting
}