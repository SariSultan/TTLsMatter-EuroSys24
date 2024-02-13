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

//#define debug_limit_nr_accesses //used to limit the number of accesses for debugging

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using LinqStatistics;
using MathNet.Numerics.Statistics;
using TTLsMatter.Common.GCHelpers;
using TTLsMatter.Common.Hashing;
using TTLsMatter.Common.Statistics;
using TTLsMatter.Datasets.Common.Types;
using TTLsMatter.Datasets.Factory;
using TTLsMatter.HyperLogLog;
using TTLsMatter.WSS.Exact;

namespace TTLsMatter.Benchmarks;

public static class WSSBenchmarks
{
#if debug_limit_nr_accesses
    private static bool DEBUG_limitNrAccesses = true;
    private const long DEBUG_NUM_ACCESSES = 1000 * 1000 * 1000;
    private static int BatchSize = (DEBUG_limitNrAccesses) ? 1000 * 1000 : 10 * 1000 * 1000;
#else
    private static int BatchSize = 10 * 1000 * 1000;
#endif

    #region WSS_EXACT_NOTTL

    /// <summary>
    /// This outputs a csv per access trace inside outputdirc/tracename
    /// </summary>
    /// <param name="timeperiod"></param>
    public static void WssExactNoTTL(string outputDirectory, uint timeperiod)
    {
        Directory.CreateDirectory(outputDirectory);

        var dst = DataSetType.FilteredTwitter;
        var traces = DatasetFactory.GetTraceFiles(dst);

        /*get the total number of requests for all traces*/
        long aggreggate_num_requests_all_traces = GetAggreggateNumRequests(traces, dst);


        long progress_aggreggate = 0;
        var aggreggate_timer = Stopwatch.StartNew();
        foreach (var trace in traces)
        {
            GCHelper.COLLECTMAX();

            var traceName = DatasetFactory.GetTraceFileName(trace, dst);

            var traceDir = Path.Combine(outputDirectory, traceName);
            Directory.CreateDirectory(traceDir);
            var outputFileName = Path.Combine(traceDir, $"{traceName}-WSS-Exact-NoTTL.csv");


            var reader = DatasetFactory.GetReader(dst, trace, null, null, null, BatchSize);
            var startTimestamp = reader.GetFirstAndLastRequest(trace).Item1.Timestamp;
            var currentTimestamp = startTimestamp;
            var nextTimeStamp = startTimestamp + timeperiod;

            var total_per_cluster = reader.GetNumberOfRequests();
            long progress_per_cluster = 0;

            if (File.Exists(outputFileName))
            {
                Console.WriteLine($"Output file already exists [{outputFileName}] (skipped)");
                progress_aggreggate += total_per_cluster;
                continue;
            }

            var wss = new WssCalculatorOptimized(false, 1);
            var csv = new StringBuilder();
            csv.AppendLine("Time(s), NumAccesses, Cardinality, AvgBlockSize, WssAvgBlockSize, WssVbs");

            while (true)
            {
                var items = reader.GetBatch();
                if (items == -1) break;

                /*Processes*/
                for (int i = 0; i < items; i++)
                {
                    var req = reader.Requests[i];
                    if (req.Timestamp >= nextTimeStamp)
                    {
                        /*output line*/
                        csv.AppendLine(
                            $"{currentTimestamp}, {progress_per_cluster}, " +
                            $"{wss.GetCardinality(0)}, {wss.AvgBlockSize}, " +
                            $"{wss.GetWss_VariableBlockSize_RunningAverage(0)}, " +
                            $"{wss.GetWss_VariableBlockSize(0)}");

                        /*increase next time*/
                        while (currentTimestamp < req.Timestamp)
                        {
                            currentTimestamp = nextTimeStamp;
                            nextTimeStamp += timeperiod;
                        }
                    }

                    wss.AddRequest(req);
                    progress_per_cluster++;
#if debug_limit_nr_accesses
                    if (DEBUG_limitNrAccesses)
                        if (progress_per_cluster >= DEBUG_NUM_ACCESSES)
                            break;
#endif
                }

                progress_aggreggate += items;
                UpdateProgress($"{nameof(WssExactNoTTL)}", progress_aggreggate, aggreggate_timer,
                    aggreggate_num_requests_all_traces, progress_per_cluster,
                    total_per_cluster, traceName);
#if debug_limit_nr_accesses
                if (DEBUG_limitNrAccesses)
                    if (progress_per_cluster >= DEBUG_NUM_ACCESSES)
                        break;
#endif
            } //!while 

            Console.WriteLine();
            /*output statistics for the last time entry*/
            csv.AppendLine(
                $"{currentTimestamp}, {progress_per_cluster}, " +
                $"{wss.GetCardinality(0)}, {wss.AvgBlockSize}, " +
                $"{wss.GetWss_VariableBlockSize_RunningAverage(0)}, " +
                $"{wss.GetWss_VariableBlockSize(0)}");

            /*output the csv file*/
            File.WriteAllText(outputFileName, csv.ToString());
            Console.WriteLine($"[AE {DateTime.Now}] WSS File Outputted:\n" +
                              $"\tFILE PATH: [{outputFileName}]\n" +
                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(outputFileName)}]");
        } //!foreach (var trace in traces)
    }

    internal class ExactNoTTLEntry
    {
        public static Dictionary<uint, ExactNoTTLEntry> ParseCsvFile(string filePath)
        {
            var lines = File.ReadAllLines(filePath);
            var toRet = new Dictionary<uint, ExactNoTTLEntry>(lines.Length);
            for (int i = 1; i < lines.Length; i++) //skip header
            {
                var line = lines[i];
                if (string.IsNullOrEmpty(line)) continue;
                var e = ParseLine(line, i, filePath);
                toRet.Add(e.Time, e);
            }

            return toRet;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ExactNoTTLEntry ParseLine(string line, int lineIdx, string filepath)
        {
            //[0]Time(s), [1]NumAccesses, [2]Cardinality, [3]AvgBlockSize, [4]WssAvgBlockSize, [5]WssVbs

            var parts = line.Split(',');
            if (parts.Length != 6)
                throw new Exception(
                    $"Error parsing {nameof(ExactNoTTLEntry)} expected 6 parts while it has {parts.Length} at line {lineIdx} from [{filepath}]");

            return new ExactNoTTLEntry()
            {
                Time = uint.Parse(parts[0]),
                NumAccesses = long.Parse(parts[1]),
                Cardinality = long.Parse(parts[2]),
                AvgBlockSize = double.Parse(parts[3]),
                WssAvgBlockSize = long.Parse(parts[4]),
                WssVbs = long.Parse(parts[5])
            };
        }

        public uint Time;
        public long NumAccesses;
        public long Cardinality;
        public double AvgBlockSize;
        public long WssAvgBlockSize;
        public long WssVbs;
    }

    #endregion WSS_EXACT_NOTTL

    #region WSS_EXACT_TTL

    public static void WssExactTTL(string outputDirectory, uint timeperiod)
    {
        Directory.CreateDirectory(outputDirectory);

        var dst = DataSetType.FilteredTwitter;
        var traces = DatasetFactory.GetTraceFiles(dst);

        /*get the total number of requests for all traces*/
        long aggreggate_num_requests_all_traces = GetAggreggateNumRequests(traces, dst);


        long progress_aggreggate = 0;
        var aggreggate_timer = Stopwatch.StartNew();
        foreach (var trace in traces)
        {
            GCHelper.COLLECTMAX();

            var traceName = DatasetFactory.GetTraceFileName(trace, dst);

            var traceDir = Path.Combine(outputDirectory, traceName);
            Directory.CreateDirectory(traceDir);
            var outputFileName = Path.Combine(traceDir, $"{traceName}-WSS-Exact-TTL.csv");


            var reader = DatasetFactory.GetReader(dst, trace, null, null, null, BatchSize);
            var startTimestamp = reader.GetFirstAndLastRequest(trace).Item1.Timestamp;
            var currentTimestamp = startTimestamp;
            var nextTimeStamp = startTimestamp + timeperiod;

            var total_per_cluster = reader.GetNumberOfRequests();
            long progress_per_cluster = 0;

            if (File.Exists(outputFileName))
            {
                Console.WriteLine($"Output file already exists [{outputFileName}] (skipped)");
                progress_aggreggate += total_per_cluster;
                continue;
            }

            var wss = new WssCalculatorOptimized(true, 1);
            var csv = new StringBuilder();
            csv.AppendLine("Time(s), NumAccesses, Cardinality, AvgBlockSize, WssAvgBlockSize, WssVbs");

            uint lastTime = 0;
            while (true)
            {
                var items = reader.GetBatch();
                if (items == -1) break;
                lastTime = reader.Requests[items - 1].Timestamp;
                /*Processes*/
                for (int i = 0; i < items; i++)
                {
                    var req = reader.Requests[i];
                    if (req.Timestamp >= nextTimeStamp)
                    {
                        /*output line*/
                        csv.AppendLine(
                            $"{currentTimestamp}, {progress_per_cluster}, " +
                            $"{wss.GetCardinality(nextTimeStamp)}, {wss.AvgBlockSize}, " +
                            $"{wss.GetWss_VariableBlockSize_RunningAverage(nextTimeStamp)}, " +
                            $"{wss.GetWss_VariableBlockSize(nextTimeStamp)}");

                        /*increase next time*/
                        while (currentTimestamp < req.Timestamp)
                        {
                            currentTimestamp = nextTimeStamp;
                            nextTimeStamp += timeperiod;
                        }
                    }

                    wss.AddRequest(req);
                    progress_per_cluster++;
                }

                progress_aggreggate += items;
                UpdateProgress($"{nameof(WssExactTTL)}", progress_aggreggate, aggreggate_timer,
                    aggreggate_num_requests_all_traces, progress_per_cluster,
                    total_per_cluster, traceName);

#if debug_limit_nr_accesses
                if (DEBUG_limitNrAccesses)
                    if (progress_per_cluster >= DEBUG_NUM_ACCESSES)
                        break;
#endif
            } //!while 

            Console.WriteLine();
            /*output statistics for the last time entry*/
            csv.AppendLine(
                $"{currentTimestamp}, {progress_per_cluster}, " +
                $"{wss.GetCardinality(lastTime)}, {wss.AvgBlockSize}, " +
                $"{wss.GetWss_VariableBlockSize_RunningAverage(lastTime)}, " +
                $"{wss.GetWss_VariableBlockSize(lastTime)}");

            /*output the csv file*/
            File.WriteAllText(outputFileName, csv.ToString());
            Console.WriteLine($"[AE {DateTime.Now}] WSS File Outputted:\n" +
                              $"\tFILE PATH: [{outputFileName}]\n" +
                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(outputFileName)}]");
        }
    }

    internal class ExactTTLEntry
    {
        public static Dictionary<uint, ExactTTLEntry> ParseCsvFile(string filePath)
        {
            var lines = File.ReadAllLines(filePath);
            var toRet = new Dictionary<uint, ExactTTLEntry>(lines.Length);
            for (int i = 1; i < lines.Length; i++) //skip header
            {
                var line = lines[i];
                if (string.IsNullOrEmpty(line)) continue;
                var e = ParseLine(line, i, filePath);
                toRet.Add(e.Time, e);
            }

            return toRet;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ExactTTLEntry ParseLine(string line, int lineIdx, string filepath)
        {
            //[0]Time(s), [1]NumAccesses, [2]Cardinality, [3]AvgBlockSize, [4]WssAvgBlockSize, [5]WssVbs

            var parts = line.Split(',');
            if (parts.Length != 6)
                throw new Exception(
                    $"Error parsing {nameof(ExactTTLEntry)} expected 6 parts while it has {parts.Length} at line {lineIdx} from [{filepath}]");

            return new ExactTTLEntry()
            {
                Time = uint.Parse(parts[0]),
                NumAccesses = long.Parse(parts[1]),
                Cardinality = long.Parse(parts[2]),
                AvgBlockSize = double.Parse(parts[3]),
                WssAvgBlockSize = long.Parse(parts[4]),
                WssVbs = long.Parse(parts[5])
            };
        }

        public uint Time;
        public long NumAccesses;
        public long Cardinality;
        public double AvgBlockSize;
        public long WssAvgBlockSize;
        public long WssVbs;
    }

    #endregion WSS_EXACT_TTL

    #region HLL_NOTTL

    internal class HllNoTTLBenchInstance
    {
        public HllBasicDense hll;
        public Stopwatch timer;
        public byte hll_precision;
        public StringBuilder report;
        public string outputFileName;
        public long totalreqsprocessed = 0;
    }

    public static void WssHLLNoTTL(string outputDirectory, uint timeperiod, bool OVERWRITE)
    {
        Directory.CreateDirectory(outputDirectory);

        byte start_hll_precision = 4;
        byte end_hll_precision = 16;

        var dst = DataSetType.FilteredTwitter;
        var traces = DatasetFactory.GetTraceFiles(dst);

        /*get the total number of requests for all traces*/
        long aggreggate_num_requests_all_traces = GetAggreggateNumRequests(traces, dst);


        long progress_aggreggate = 0;
        var aggreggate_timer = Stopwatch.StartNew();
        foreach (var trace in traces)
        {
            GCHelper.COLLECTMAX();

            var traceName = DatasetFactory.GetTraceFileName(trace, dst);

            var traceDir = Path.Combine(outputDirectory, traceName);
            Directory.CreateDirectory(traceDir);

            var hlls = new List<HllNoTTLBenchInstance>();
            for (var b = start_hll_precision; b <= end_hll_precision; b++)
            {
                var outputFileName = Path.Combine(traceDir, $"{traceName}-WSS-HLL-b{b}-NoTTL.csv");
                if (File.Exists(outputFileName) && !OVERWRITE)
                {
                    Console.WriteLine($"Skipping {outputFileName} already exists.");
                    continue;
                }
                else
                {
                    var hll = new HllBasicDense(b);
                    var csv = new StringBuilder();
                    csv.AppendLine(
                        "Time(s), NumAccesses, Cardinality, AvgBlockSize, WssAvgBlockSize, TimeNeededNoIO(s), ThroughputNoIO(r/s), IoTime(s), ThroughputWithIo(r/s)");

                    var entry = new HllNoTTLBenchInstance()
                    {
                        hll = hll,
                        report = csv,
                        hll_precision = b,
                        outputFileName = outputFileName,
                        timer = new Stopwatch(),
                        totalreqsprocessed = 0
                    };
                    hlls.Add(entry);
                }
            }


            var reader = DatasetFactory.GetReader(dst, trace, null, null, null, BatchSize);
            var startTimestamp = reader.GetFirstAndLastRequest(trace).Item1.Timestamp;
            var currentTimestamp = startTimestamp;
            var nextTimeStamp = startTimestamp + timeperiod;

            var total_per_cluster = reader.GetNumberOfRequests();
            long progress_per_cluster = 0;

            if (!hlls.Any())
            {
                Console.WriteLine($"Output files already exists [{traceName}] (skipped)");
                progress_aggreggate += total_per_cluster;
                continue;
            }

            var avgBlockSizeDic = new Dictionary<uint, double>();

            double AvgBlockSize = 0;
            long NumInsertedItems = 0;

            var iotimer = new Stopwatch();
            while (true)
            {
                iotimer.Start();
                var items = reader.GetBatch();
                iotimer.Stop();
                if (items == -1) break;
                /*Processes*/

                //compute avgBlockSize
                uint currentTimestampLocal = currentTimestamp, nextTimestampLocal = nextTimeStamp;
                for (int i = 0; i < items; i++)
                {
                    var req = reader.Requests[i];
                    if (req.Timestamp >= nextTimestampLocal)
                    {
                        // Console.WriteLine("Adding to avgBlockSizeDic");
                        // Console.WriteLine($"CTS: {currentTimestamp} CTS_local: {currentTimestampLocal}");
                        //Console.WriteLine($"NTS: {nextTimeStamp} NTS_local: {nextTimestampLocal}");

                        avgBlockSizeDic.Add(currentTimestampLocal, AvgBlockSize);

                        /*increase next time*/
                        while (req.Timestamp >= nextTimestampLocal)
                        {
                            currentTimestampLocal = nextTimestampLocal;
                            nextTimestampLocal += timeperiod;
                        }
                    }

                    AvgBlockSize = ((req.BlockSize - AvgBlockSize) / ++NumInsertedItems) + AvgBlockSize;
                }

                /*Add the batch to each of the HLLs*/
                foreach (var hll in hlls)
                {
                    /*Reset the time to the beginning of the period*/
                    currentTimestampLocal = currentTimestamp;
                    nextTimestampLocal = nextTimeStamp;

                    hll.timer.Start();
                    for (int i = 0; i < items; i++)
                    {
                        var req = reader.Requests[i];
                        if (req.Timestamp >= nextTimestampLocal)
                        {
                            hll.timer.Stop();
                            /*output line*/
                            var count = hll.hll.Count();
                            var avgBlockSize = avgBlockSizeDic[currentTimestampLocal];
                            hll.report.AppendLine(
                                $"{currentTimestampLocal}, {hll.totalreqsprocessed}, " +
                                $"{count}, {avgBlockSize}, " +
                                $"{count * avgBlockSize}, {hll.timer.Elapsed.TotalSeconds}, {hll.totalreqsprocessed / hll.timer.Elapsed.TotalSeconds}, {iotimer.Elapsed.TotalSeconds}, {hll.totalreqsprocessed / (hll.timer.Elapsed.TotalSeconds + iotimer.Elapsed.TotalSeconds)}");

                            /*increase next time*/
                            while (req.Timestamp >= nextTimestampLocal)
                            {
                                currentTimestampLocal = nextTimestampLocal;
                                nextTimestampLocal += timeperiod;
                            }

                            hll.timer.Start();
                        }

                        hll.hll.AddHash(req.KeyHash);
                        hll.totalreqsprocessed++;
#if debug_limit_nr_accesses
                        if (DEBUG_limitNrAccesses)
                            if (hll.totalreqsprocessed >= DEBUG_NUM_ACCESSES)
                                break;
#endif
                    }

                    hll.timer.Stop();
                }

                //Console.WriteLine($"CTS: {currentTimestamp} CTS_local: {currentTimestampLocal}");
                // Console.WriteLine($"NTS: {nextTimeStamp} NTS_local: {nextTimestampLocal}");
                currentTimestamp = currentTimestampLocal;
                nextTimeStamp = nextTimestampLocal;


                progress_aggreggate += items;
                progress_per_cluster += items;

                UpdateProgress($"{nameof(WssHLLNoTTL)}", progress_aggreggate, aggreggate_timer,
                    aggreggate_num_requests_all_traces, progress_per_cluster,
                    total_per_cluster, traceName);
#if debug_limit_nr_accesses
                if (DEBUG_limitNrAccesses)
                    if (progress_per_cluster >= DEBUG_NUM_ACCESSES)
                        break;
#endif
            } //!while 

            avgBlockSizeDic.Add(currentTimestamp, AvgBlockSize);

            Console.WriteLine();
            /*output statistics for the last time entry*/
            foreach (var hll in hlls)
            {
                /*output the last line*/
                var count = hll.hll.Count();
                var avgBlockSize = avgBlockSizeDic[currentTimestamp];
                hll.report.AppendLine(
                    $"{currentTimestamp}, {hll.totalreqsprocessed}, " +
                    $"{count}, {avgBlockSize}, " +
                    $"{count * avgBlockSize}, {hll.timer.Elapsed.TotalSeconds}, {hll.totalreqsprocessed / hll.timer.Elapsed.TotalSeconds}, {iotimer.Elapsed.TotalSeconds}, {hll.totalreqsprocessed / (hll.timer.Elapsed.TotalSeconds + iotimer.Elapsed.TotalSeconds)}");

                File.WriteAllText(hll.outputFileName, hll.report.ToString());
                Console.WriteLine($"[AE {DateTime.Now}] WSS File Outputted:\n" +
                                  $"\tFILE PATH: [{hll.outputFileName}]\n" +
                                  $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(hll.outputFileName)}]");
            }
        }
    }

    internal class HllNoTTLEntry
    {
        public static Dictionary<uint, HllNoTTLEntry> ParseCsvFile(string filePath)
        {
            /*Get the precision from the filename. (expected format: cluster4-WSS-HLL-b5-NoTTL)*/
            var precision = int.Parse(Path.GetFileNameWithoutExtension(filePath).Split('-')[3].Replace("b", ""));
            if (precision < 4 || precision > 16)
                throw new Exception($"Cannot parse precision from file name {filePath}");
            var lines = File.ReadAllLines(filePath);
            var toRet = new Dictionary<uint, HllNoTTLEntry>(lines.Length);
            for (int i = 1; i < lines.Length; i++) //skip header
            {
                var line = lines[i];
                if (string.IsNullOrEmpty(line)) continue;
                var entry = ParseLine(line, i, filePath);
                entry.Precision = precision;
                toRet.Add(entry.Time, entry);
            }

            return toRet;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static HllNoTTLEntry ParseLine(string line, int lineIdx, string filepath)
        {
            //"[0]Time(s), [1]NumAccesses, [2]Cardinality, [3]AvgBlockSize, [4]WssAvgBlockSize, [5]TimeNeeded(s), [6]Throughput(r/s)"
            var parts = line.Split(',');
            if (parts.Length != 9)
                throw new Exception(
                    $"Error parsing {nameof(HllNoTTLEntry)} expected 7 parts while it has {parts.Length} at line {lineIdx} from [{filepath}]");

            return new HllNoTTLEntry()
            {
                Time = uint.Parse(parts[0]),
                NumAccesses = long.Parse(parts[1]),
                Cardinality = long.Parse(parts[2]),
                AvgBlockSize = double.Parse(parts[3]),
                WssAvgBlockSize = double.Parse(parts[4]),
                TimeNeededNoIo = double.Parse(parts[5]),
                ThroughputNoIo = double.Parse(parts[6]),
                TimeNeededIoOnly = double.Parse(parts[7]),
                ThroughputWithIO = double.Parse(parts[8]),
            };
        }

        public int Precision;
        public uint Time;
        public long NumAccesses;
        public long Cardinality;
        public double AvgBlockSize;
        public double WssAvgBlockSize;
        public double TimeNeededNoIo;
        public double ThroughputNoIo;
        public double TimeNeededIoOnly;
        public double ThroughputWithIO;
    }

    #endregion HLL_NOTTL

    #region HLL_TTL

    internal class HllTTLBenchInstance
    {
        public HllBasicDenseTtl_Sparce hll;
        public Stopwatch timer;
        public byte hll_precision;
        public StringBuilder report;
        public string outputFileName;
        public long totalreqsprocessed = 0;
    }


    public static void WssHLLTTL(string outputDirectory, uint timeperiod, bool OVERWRITE)
    {
        Directory.CreateDirectory(outputDirectory);

        byte start_hll_precision = 4;
        //byte start_hll_precision = 12;
        byte end_hll_precision = 16;

        var dst = DataSetType.FilteredTwitter;
        var traces = DatasetFactory.GetTraceFiles(dst);

        /*get the total number of requests for all traces*/
        long aggreggate_num_requests_all_traces = GetAggreggateNumRequests(traces, dst);


        long progress_aggreggate = 0;
        var aggreggate_timer = Stopwatch.StartNew();
        foreach (var trace in traces)
        {
            GCHelper.COLLECTMAX();

            var traceName = DatasetFactory.GetTraceFileName(trace, dst);

            var traceDir = Path.Combine(outputDirectory, traceName);
            Directory.CreateDirectory(traceDir);

            var hlls = new List<HllTTLBenchInstance>();
            for (var b = start_hll_precision; b <= end_hll_precision; b++)
            {
                var outputFileName = Path.Combine(traceDir, $"{traceName}-WSS-HLL-b{b}-TTL.csv");
                if (File.Exists(outputFileName) && !OVERWRITE)
                {
                    Console.WriteLine($"Skipping {outputFileName} already exists.");
                    continue;
                }
                else
                {
                    var hll = new HllBasicDenseTtl_Sparce(b, 0, (byte)(64 - b));
                    hll.BlockSize = 1;
                    var csv = new StringBuilder();
                    csv.AppendLine(
                        "Time(s), NumAccesses, Cardinality, AvgBlockSize, WssAvgBlockSize, TimeNeededNoIO(s), ThroughputNoIO(r/s), TimeNeededIoOnly(s), ThroughputWithIO(r/s), Size (bytes) Static, Size(byte) Dynamic, Savings dynamic (%)");

                    var entry = new HllTTLBenchInstance()
                    {
                        hll = hll,
                        report = csv,
                        hll_precision = b,
                        outputFileName = outputFileName,
                        timer = new Stopwatch(),
                        totalreqsprocessed = 0
                    };
                    hlls.Add(entry);
                }
            }


            var reader = DatasetFactory.GetReader(dst, trace, null, null, null, BatchSize);
            var startTimestamp = reader.GetFirstAndLastRequest(trace).Item1.Timestamp;
            var currentTimestamp = startTimestamp;
            var nextTimeStamp = startTimestamp + timeperiod;

            var total_per_cluster = reader.GetNumberOfRequests();
            long progress_per_cluster = 0;

            if (!hlls.Any())
            {
                Console.WriteLine($"Output files already exists [{traceName}] (skipped)");
                progress_aggreggate += total_per_cluster;
                continue;
            }

            var avgBlockSizeDic = new Dictionary<uint, double>();

            double AvgBlockSize = 0;
            long NumInsertedItems = 0;
            uint lastTime = 0;

            var ioTimer = new Stopwatch();
            while (true)
            {
                ioTimer.Start();
                var items = reader.GetBatch();
                ioTimer.Stop();
                if (items == -1) break;
                lastTime = reader.Requests[items - 1].Timestamp;
                /*Processes*/

                //compute avgBlockSize
                uint currentTimestampLocal = currentTimestamp, nextTimestampLocal = nextTimeStamp;
                for (int i = 0; i < items; i++)
                {
                    var req = reader.Requests[i];
                    if (req.Timestamp >= nextTimestampLocal)
                    {
                        avgBlockSizeDic.Add(currentTimestampLocal, AvgBlockSize);

                        /*increase next time*/
                        while (req.Timestamp >= nextTimestampLocal)
                        {
                            currentTimestampLocal = nextTimestampLocal;
                            nextTimestampLocal += timeperiod;
                        }
                    }

                    AvgBlockSize = ((req.BlockSize - AvgBlockSize) / ++NumInsertedItems) + AvgBlockSize;
                }

                /*Add the batch to each of the HLLs*/
                foreach (var hll in hlls)
                {
                    /*Reset the time to the beginning of the period*/
                    currentTimestampLocal = currentTimestamp;
                    nextTimestampLocal = nextTimeStamp;

                    hll.timer.Start();
                    for (int i = 0; i < items; i++)
                    {
                        var req = reader.Requests[i];
                        if (req.Timestamp >= nextTimestampLocal)
                        {
                            hll.timer.Stop();
                            /*output line*/
                            var count = hll.hll.EvictExpiredBucketsAndCount(nextTimestampLocal);
                            var avgBlockSize = avgBlockSizeDic[currentTimestampLocal];
                            var sizeStatic = hll.hll.SerializeStatic().Length;
                            var sizeDynamic = hll.hll.SerializeDynamic().Length;
                            var savingsDynamic = (sizeStatic - sizeDynamic) * 100.0 / sizeStatic;
                            hll.report.AppendLine(
                                $"{currentTimestampLocal}, {hll.totalreqsprocessed}, " +
                                $"{count}, {avgBlockSize}, " +
                                $"{count * avgBlockSize}, {hll.timer.Elapsed.TotalSeconds}, {hll.totalreqsprocessed / hll.timer.Elapsed.TotalSeconds}," +
                                $" {ioTimer.Elapsed.TotalSeconds}, {hll.totalreqsprocessed / (hll.timer.Elapsed.TotalSeconds + ioTimer.Elapsed.TotalSeconds)}, " +
                                $"{sizeStatic}, {sizeDynamic}, {savingsDynamic:f6}");

                            /*increase next time*/
                            while (req.Timestamp >= nextTimestampLocal)
                            {
                                currentTimestampLocal = nextTimestampLocal;
                                nextTimestampLocal += timeperiod;
                            }

                            hll.timer.Start();
                        }

                        hll.hll.AddHash(req.KeyHash, req.EvictionTime);
                        hll.totalreqsprocessed++;
                    }

                    hll.timer.Stop();
                }

                currentTimestamp = currentTimestampLocal;
                nextTimeStamp = nextTimestampLocal;


                progress_aggreggate += items;
                progress_per_cluster += items;

                UpdateProgress($"{nameof(WssHLLTTL)}", progress_aggreggate, aggreggate_timer,
                    aggreggate_num_requests_all_traces, progress_per_cluster,
                    total_per_cluster, traceName);

#if debug_limit_nr_accesses
                if (DEBUG_limitNrAccesses)
                    if (progress_per_cluster >= DEBUG_NUM_ACCESSES)
                        break;
#endif
            } //!while 

            avgBlockSizeDic.Add(currentTimestamp, AvgBlockSize);

            Console.WriteLine();
            /*output statistics for the last time entry*/
            foreach (var hll in hlls)
            {
                /*output the last line*/
                var count = hll.hll.EvictExpiredBucketsAndCount(lastTime);
                var avgBlockSize = avgBlockSizeDic[currentTimestamp];
                var sizeStatic = hll.hll.SerializeStatic().Length;
                var sizeDynamic = hll.hll.SerializeDynamic().Length;
                var savingsDynamic = (sizeStatic - sizeDynamic) * 100.0 / sizeStatic;

                hll.report.AppendLine(
                    $"{currentTimestamp}, {hll.totalreqsprocessed}, " +
                    $"{count}, {avgBlockSize}, " +
                    $"{count * avgBlockSize}, {hll.timer.Elapsed.TotalSeconds}, {hll.totalreqsprocessed / hll.timer.Elapsed.TotalSeconds}," +
                    $" {ioTimer.Elapsed.TotalSeconds}, {hll.totalreqsprocessed / (hll.timer.Elapsed.TotalSeconds + ioTimer.Elapsed.TotalSeconds)}, " +
                    $"{sizeStatic}, {sizeDynamic}, {savingsDynamic:f6}");

                File.WriteAllText(hll.outputFileName, hll.report.ToString());
                Console.WriteLine($"[AE {DateTime.Now}] WSS File Outputted:\n" +
                                  $"\tFILE PATH: [{hll.outputFileName}]\n" +
                                  $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(hll.outputFileName)}]");
            }
        }
    }

    internal class HllTTLEntry
    {
        public static Dictionary<uint, HllTTLEntry> ParseCsvFile(string filePath)
        {
            /*Get the precision from the filename. (expected format: clusterXX-WSS-HLL-b4-TTL)*/
            var precision = int.Parse(Path.GetFileNameWithoutExtension(filePath).Split('-')[3].Replace("b", ""));
            if (precision < 4 || precision > 16)
                throw new Exception($"Cannot parse precision from file name {filePath}");
            var lines = File.ReadAllLines(filePath);
            var toRet = new Dictionary<uint, HllTTLEntry>(lines.Length);
            for (int i = 1; i < lines.Length; i++) //skip header
            {
                var line = lines[i];
                if (string.IsNullOrEmpty(line)) continue;
                var entry = ParseLine(line, i, filePath);
                entry.Precision = precision;
                toRet.Add(entry.Time, entry);
            }

            return toRet;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static HllTTLEntry ParseLine(string line, int lineIdx, string filepath)
        {
            //[0]Time(s), [1]NumAccesses, [2]Cardinality, [3]AvgBlockSize, [4]WssAvgBlockSize, [5]TimeNeeded(s)
            //, [6]Throughput(r/s), [7]Size (bytes) Static, [8]Size(byte) Dynamic, [9]Savings dynamic (%)
            var parts = line.Split(',');
            if (parts.Length != 12)
                throw new Exception(
                    $"Error parsing {nameof(HllTTLEntry)} expected 10 parts while it has {parts.Length} at line {lineIdx} from [{filepath}]");

            return new HllTTLEntry()
            {
                Time = uint.Parse(parts[0]),
                NumAccesses = long.Parse(parts[1]),
                Cardinality = long.Parse(parts[2]),
                AvgBlockSize = double.Parse(parts[3]),
                WssAvgBlockSize = double.Parse(parts[4]),
                TimeNeededNoIO = double.Parse(parts[5]),
                ThroughputNoIO = double.Parse(parts[6]),
                TimeNeededIoOnly = double.Parse(parts[7]),
                ThroughputWithIO = double.Parse(parts[8]),
                SizeStatic = int.Parse(parts[9]),
                SizeDynamic = int.Parse(parts[10]),
                SavingsPercentage = double.Parse(parts[11]),
            };
        }

        public int Precision;
        public uint Time;
        public long NumAccesses;
        public long Cardinality;
        public double AvgBlockSize;
        public double WssAvgBlockSize;
        public double TimeNeededNoIO;
        public double ThroughputNoIO;
        public double TimeNeededIoOnly;
        public double ThroughputWithIO;
        public int SizeStatic;
        public int SizeDynamic;
        public double SavingsPercentage;
    }

    #endregion HLL_TTL

    #region BENCHMARKING HELPERS

    public static void UpdateProgress(string prefix, long progress_aggreggate, Stopwatch aggreggate_timer,
        long aggreggate_num_requests_all_traces, long progress_per_cluster, long total_per_cluster, string traceName)
    {
        var global_speed = progress_aggreggate / aggreggate_timer.Elapsed.TotalSeconds;
        var global_eta_seconds = (aggreggate_num_requests_all_traces - progress_aggreggate) / global_speed;
        var eta_timespan = TimeSpan.FromSeconds(global_eta_seconds);

        var localProgressPerc = progress_per_cluster * 100.0 / total_per_cluster;
        var globalProgressPerc = progress_aggreggate * 100.0 / aggreggate_num_requests_all_traces;
        Console.Write($"\r{DateTime.Now}: {prefix} - {traceName}." +
                      $" L ({localProgressPerc:f2}%) [{progress_per_cluster:n0}/{total_per_cluster:n0}]" +
                      $" G ({globalProgressPerc:f2}%) [{progress_aggreggate:n0}/{aggreggate_num_requests_all_traces:n0}]" +
                      $" Speed: {global_speed:n0} r/s" +
                      $" ETA: {eta_timespan.Hours}H:{eta_timespan.Minutes}M:{eta_timespan.Seconds}s");
    }

    public static long GetAggreggateNumRequests(List<string> traces, DataSetType dst
    )
    {
        long aggreggate_num_requests_all_traces = 0;
        foreach (var trace in traces)
        {
            var reader = DatasetFactory.GetReader(dst, trace, null, null, null, 1);
            aggreggate_num_requests_all_traces += reader.GetNumberOfRequests();
        }

        return aggreggate_num_requests_all_traces;
    }

    #endregion BENCHMARKING HELPERS

    #region RESULTS ANALYSIS

    /// <summary>
    /// Ths input directory contains the following results, for each cluster (as subdirectory).
    /// 1. NoTTL-Exact
    /// 2. TTL-Exact
    /// 3. NoTTL-HLL (different precisions)
    /// 4. TTL-HLL (different precisions)
    ///-----------------------------------------------
    /// We are interested in the following metrics.
    ///
    /// A. GAP (savings): the difference between WSS-NoTTL and WSS-TTL (HWM)
    ///     This will results in a CSV called Gap-Exact-WSS-nottl-vs-ttl.csv
    ///     <clusterid, exact-nottl, exact-ttl, gap(bytes), savings(gap)%>
    /// 
    /// B. Error_HLL(precision): Defined as average per hour (WSS_TTL_Exact - WSS_TTL_HLL)/WSS_TTL_EXACT
    ///     This will result in two CSVs: Error-HLL-TTL.csv
    ///     Question: should the error be for HWM only or all points? 
    ///     <precision, min-error, max, average, median, 25th, 75th, stdev, 95thmin, 95thmax>
    /// 
    /// C. Throughput of nonttl-hll
    ///     this results in throughput-hll-nottl.csv
    ///     <precision, min-throughput, max, average, median, 25th, 75th, stdev, 95thmin, 95thmax>
    /// 
    /// D. throughput of ttl-hll
    ///      This results in throughput-hll-ttl.csv
    ///      <precision, min-throughput, max, average, median, 25th, 75th, stdev, 95thmin, 95thmax>
    ///
    /// E. Memory savings:
    ///     This should results in memory-hll-ttl.csv
    ///     <precision, static-size, dynamic-min, max, average, median, 25th, 27th, stdev, 95thmin, 95thmax>
    ///  
    /// </summary>
    /// <param name="inputDirectory"></param>
    public static void ResultsAnalysis(string inputDirectory)
    {
        if (!Directory.Exists(inputDirectory))
        {
            Console.WriteLine(
                $"Error in [{nameof(ResultsAnalysis)}()]: cannot analyze WSS results because the results directory does not exist [{inputDirectory}]. Make sure to run the experiments.");
            return;
        }

        var clusters = Directory.GetDirectories(inputDirectory);

        int hll_precision_start = 4, hll_precision_end = 16;

        var exact_WSS_NoTTL_dic = new Dictionary<string, Dictionary<uint, ExactNoTTLEntry>>();
        var exact_WSS_NoTTL_dic_lock = new object();

        var exact_WSS_TTL_dic = new Dictionary<string, Dictionary<uint, ExactTTLEntry>>();
        var exact_WSS_TTL_dic_lock = new object();

        var HLL_WSS_NoTTL_dic = new Dictionary<string, Dictionary<int, Dictionary<uint, HllNoTTLEntry>>>();
        var HLL_WSS_NoTTL_dic_lock = new object();

        var HLL_WSS_TTL_dic = new Dictionary<string, Dictionary<int, Dictionary<uint, HllTTLEntry>>>();
        var HLL_WSS_TTL_dic_lock = new object();

        #region PARSING FILES

        Console.WriteLine("Parsing results");
        //foreach (var cluster in clusters)
        Parallel.ForEach(clusters, cluster =>
        {
            {
                var clusterName = Path.GetFileNameWithoutExtension(cluster);
                var csvs = Directory.GetFiles(cluster, "*.csv");
                foreach (var csvPath in csvs)
                {
                    var fn = Path.GetFileNameWithoutExtension(csvPath);
                    if (fn.Contains("WSS-Exact-NoTTL"))
                    {
                        var e = ExactNoTTLEntry.ParseCsvFile(csvPath);
                        lock (exact_WSS_NoTTL_dic_lock)
                            exact_WSS_NoTTL_dic.Add(clusterName, e);
                    }
                    else if (fn.Contains("WSS-Exact-TTL"))
                    {
                        var e = ExactTTLEntry.ParseCsvFile(csvPath);
                        lock (exact_WSS_TTL_dic_lock)
                            exact_WSS_TTL_dic.Add(clusterName, e);
                    }
                    else if (fn.Contains("HLL") && fn.Contains("NoTTL"))
                    {
                        var e = HllNoTTLEntry.ParseCsvFile(csvPath);
                        var precision = e.First().Value.Precision;
                        lock (HLL_WSS_NoTTL_dic_lock)
                        {
                            if (!HLL_WSS_NoTTL_dic.ContainsKey(clusterName))
                                HLL_WSS_NoTTL_dic.Add(clusterName,
                                    new Dictionary<int, Dictionary<uint, HllNoTTLEntry>>());

                            HLL_WSS_NoTTL_dic[clusterName].Add(precision, e);
                        }
                    }
                    else if (fn.Contains("HLL") && fn.Contains("TTL"))
                    {
                        var e = HllTTLEntry.ParseCsvFile(csvPath);
                        var precision = e.First().Value.Precision;
                        lock (HLL_WSS_TTL_dic_lock)
                        {
                            if (!HLL_WSS_TTL_dic.ContainsKey(clusterName))
                                HLL_WSS_TTL_dic.Add(clusterName, new Dictionary<int, Dictionary<uint, HllTTLEntry>>());

                            HLL_WSS_TTL_dic[clusterName].Add(precision, e);
                        }
                    }
                    else
                    {
                        throw new Exception($"File name not supports {csvPath}");
                    }
                }
            }
        }); //Parallel.foreach
        Console.WriteLine("Finished parsing results");

        #endregion

        #region GAP ANALYSIS

        /*GAP Analysis*/
        // A. GAP (savings): the difference between WSS-NoTTL and WSS-TTL (HWM)
        //     This will results in a CSV called Gap-Exact-WSS-nottl-vs-ttl.csv
        //     <clusterid, exact-nottl, exact-ttl, gap(bytes), savings(gap)%>
        var gapFilePath = Path.Combine(inputDirectory, "Gap-Exact-WSS-nottl-vs-ttl.csv");

        int GetClusterId(string cn)
        {
            return int.Parse(cn.Replace("cluster", ""));
        }

        var gap_wss_exact_report = new StringBuilder();
        gap_wss_exact_report.AppendLine(
            "ClusterID, wss-exact-nottl, wss-exact-ttl(HWM), gap-hwm(bytes), gap-hwm-savings(%), wss-exact-last-point, gap-lastpoint(bytes), gap-savings-last-point(%)");
        foreach (var kvp in exact_WSS_TTL_dic.OrderBy(x => GetClusterId(x.Key)))
        {
            var clusterName = kvp.Key;
            var clusterId = GetClusterId(kvp.Key);

            /*The working set size at the last time point*/
            var exactWssNoTTL = exact_WSS_NoTTL_dic[clusterName]
                .MaxBy(x => x.Key).Value.WssAvgBlockSize;


            /*HWM*/
            var exactWssTTLhwm = kvp.Value.Max(x => x.Value.WssAvgBlockSize);
            var gap_hwm_bytes = exactWssNoTTL - exactWssTTLhwm;
            var savings_hwm_perc = gap_hwm_bytes * 100.0 / exactWssNoTTL;

            /*Last Point*/
            var exactWssTTLLastPoint = kvp.Value.MaxBy(x => x.Key)
                .Value.WssAvgBlockSize;
            var gap_lastpoint_bytes = exactWssNoTTL - exactWssTTLLastPoint;
            var savings_lastPoint_perc = gap_lastpoint_bytes * 100.0 / exactWssNoTTL;


            gap_wss_exact_report.AppendLine(
                $"{clusterId}, {exactWssNoTTL}, {exactWssTTLhwm}, {gap_hwm_bytes}, {savings_hwm_perc}, {exactWssTTLLastPoint}, {gap_lastpoint_bytes}, {savings_lastPoint_perc}");
        }

        File.WriteAllText(gapFilePath, gap_wss_exact_report.ToString());
        Console.WriteLine($"[AE {DateTime.Now}] WSS File Outputted:\n" +
                          $"\tFILE PATH: [{gapFilePath}]\n" +
                          $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(gapFilePath)}]");

        #endregion GAP ANALYSIS

        /*Accuracy*/

        string GETSTATLINE(int precision, List<double> input)
        {
            var average = input.Average();
            var median = input.Median(x => x);
            var min = input.Min();
            var max = input.Max();
            var stdev = input.StandardDeviationP();
            var ci95 = ConfidenceIntervalHelper.Get95Confidence(input);
            var ci95min = average - ci95;
            var ci95max = average + ci95;
            var twentyfifthPercentile = input.Percentile(25);
            var seventyfifthPercentile = input.Percentile(75);
            var theoritical_error = 100 * 1.04 / Math.Sqrt(Math.Pow(2, precision));
            return
                $"{precision}, {input.Count}, {min}, {max}, {median}, {average}, {stdev}, {ci95min}, {ci95max}, {twentyfifthPercentile}, {seventyfifthPercentile}, {theoritical_error}";
        }

        //For each cluster, we get the accuracy for each precision, then we present the results. 
        {
            var error_percentages = new Dictionary<int, List<double>>();
            foreach (var kvp in exact_WSS_TTL_dic)
            {
                var clusterName = kvp.Key;
                var hwmTimeEntry = kvp.Value.MaxBy(x => x.Value.WssAvgBlockSize);
                var hwmTimestamp = hwmTimeEntry.Key;
                var wssHwm = hwmTimeEntry.Value.WssAvgBlockSize;
                foreach (var hllkvp in HLL_WSS_TTL_dic[clusterName].OrderBy(x => x.Key))
                {
                    var precision = hllkvp.Key;
                    var hllWssHwm = hllkvp.Value.MaxBy(x => x.Value.WssAvgBlockSize).Value.WssAvgBlockSize;

                    var error = Math.Abs(wssHwm - hllWssHwm) * 100.0 / wssHwm;
                    if (!error_percentages.ContainsKey(precision)) error_percentages.Add(precision, new List<double>());

                    error_percentages[precision].Add(error);

                    //debug only
                    Console.WriteLine($"[+] DEBUG: {clusterName} HLL[{precision}] WSS_TTL error: {error}%");
                    if (error > 30)
                    {
                        Console.WriteLine(
                            $"\t HWMtimestamp: {hwmTimestamp} WSSttl_exact: {wssHwm} WSSttl_hll: {hllWssHwm}");
                    }
                }
            }

            var report = new StringBuilder();
            report.AppendLine(
                "Precision, NumTraces, Min(%), Max, Median, Average, Stdev, ci95min, ci95max, 25thpercentile, 75thpercentile, HLL_standardError(%)");
            foreach (var kvp in error_percentages.OrderBy(x => x.Key))
            {
                report.AppendLine(GETSTATLINE(kvp.Key, kvp.Value));
            }

            var outputFilePath = Path.Combine(inputDirectory, "wss-hll-ttl-error.csv");
            File.WriteAllText(outputFilePath, report.ToString());
            Console.WriteLine($"[AE {DateTime.Now}] WSS File Outputted:\n" +
                              $"\tFILE PATH: [{outputFilePath}]\n" +
                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(outputFilePath)}]");
        }

        {
            /*TTL - throughput - no IO*/
            var throughput_values = new Dictionary<int, List<double>>();
            foreach (var kvp in HLL_WSS_TTL_dic)
            {
                foreach (var innerkvp in kvp.Value.OrderBy(x => x.Key))
                {
                    var precision = innerkvp.Key;
                    var throughput = innerkvp.Value.MaxBy(x => x.Key).Value.ThroughputNoIO;

                    if (!throughput_values.ContainsKey(precision)) throughput_values.Add(precision, new List<double>());

                    throughput_values[precision].Add(throughput);
                }
            }

            var report = new StringBuilder();
            report.AppendLine(
                "Precision, NumTraces, Min(%), Max, Median, Average, Stdev, ci95min, ci95max, 25thpercentile, 75thpercentile, HLL_standardError(%)");
            foreach (var kvp in throughput_values.OrderBy(x => x.Key))
                report.AppendLine(GETSTATLINE(kvp.Key, kvp.Value));

            var outputFilePath = Path.Combine(inputDirectory, "wss-hll-ttl-throughput-without-io.csv");
            File.WriteAllText(outputFilePath, report.ToString());
            Console.WriteLine($"[AE {DateTime.Now}] WSS File Outputted:\n" +
                              $"\tFILE PATH: [{outputFilePath}]\n" +
                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(outputFilePath)}]");
        }
        {
            /*TTL - throughput - with IO*/
            var throughput_values = new Dictionary<int, List<double>>();
            foreach (var kvp in HLL_WSS_TTL_dic)
            {
                foreach (var innerkvp in kvp.Value.OrderBy(x => x.Key))
                {
                    var precision = innerkvp.Key;
                    var throughput = innerkvp.Value.MaxBy(x => x.Key).Value.ThroughputWithIO;

                    if (!throughput_values.ContainsKey(precision)) throughput_values.Add(precision, new List<double>());

                    throughput_values[precision].Add(throughput);
                }
            }

            var report = new StringBuilder();
            report.AppendLine(
                "Precision, NumTraces, Min(%), Max, Median, Average, Stdev, ci95min, ci95max, 25thpercentile, 75thpercentile, HLL_standardError(%)");
            foreach (var kvp in throughput_values.OrderBy(x => x.Key))
                report.AppendLine(GETSTATLINE(kvp.Key, kvp.Value));

            var outputFilePath = Path.Combine(inputDirectory, "wss-hll-ttl-throughput-with-io.csv");
            File.WriteAllText(outputFilePath, report.ToString());
            Console.WriteLine($"[AE {DateTime.Now}] WSS File Outputted:\n" +
                              $"\tFILE PATH: [{outputFilePath}]\n" +
                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(outputFilePath)}]");
        }

        {
            /*NoTTL - throughput (without IO)*/
            var throughput_values = new Dictionary<int, List<double>>();
            foreach (var kvp in HLL_WSS_NoTTL_dic)
            {
                foreach (var innerkvp in kvp.Value.OrderBy(x => x.Key))
                {
                    var precision = innerkvp.Key;
                    var throughput = innerkvp.Value.MaxBy(x => x.Key).Value.ThroughputNoIo;

                    if (!throughput_values.ContainsKey(precision)) throughput_values.Add(precision, new List<double>());

                    throughput_values[precision].Add(throughput);
                }
            }

            var report = new StringBuilder();
            report.AppendLine(
                "Precision, NumTraces, Min(%), Max, Median, Average, Stdev, ci95min, ci95max, 25thpercentile, 75thpercentile, HLL_standardError(%)");
            foreach (var kvp in throughput_values.OrderBy(x => x.Key))
                report.AppendLine(GETSTATLINE(kvp.Key, kvp.Value));

            var outputFilePath = Path.Combine(inputDirectory, "wss-hll-nottl-throughput-without-io.csv");
            File.WriteAllText(outputFilePath, report.ToString());
            Console.WriteLine($"[AE {DateTime.Now}] WSS File Outputted:\n" +
                              $"\tFILE PATH: [{outputFilePath}]\n" +
                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(outputFilePath)}]");
        }
        {
            /*NoTTL - throughput (withIO)*/
            var throughput_values = new Dictionary<int, List<double>>();
            foreach (var kvp in HLL_WSS_NoTTL_dic)
            {
                foreach (var innerkvp in kvp.Value.OrderBy(x => x.Key))
                {
                    var precision = innerkvp.Key;
                    var throughput = innerkvp.Value.MaxBy(x => x.Key).Value.ThroughputWithIO;

                    if (!throughput_values.ContainsKey(precision)) throughput_values.Add(precision, new List<double>());

                    throughput_values[precision].Add(throughput);
                }
            }

            var report = new StringBuilder();
            report.AppendLine(
                "Precision, NumTraces, Min(%), Max, Median, Average, Stdev, ci95min, ci95max, 25thpercentile, 75thpercentile, HLL_standardError(%)");
            foreach (var kvp in throughput_values.OrderBy(x => x.Key))
                report.AppendLine(GETSTATLINE(kvp.Key, kvp.Value));

            var outputFilePath = Path.Combine(inputDirectory, "wss-hll-nottl-throughput-with-io.csv");
            File.WriteAllText(outputFilePath, report.ToString());
            Console.WriteLine($"[AE {DateTime.Now}] WSS File Outputted:\n" +
                              $"\tFILE PATH: [{outputFilePath}]\n" +
                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(outputFilePath)}]");
        }

        {
            /*dynamic memory*/
            var values = new Dictionary<int, List<double>>();
            foreach (var kvp in HLL_WSS_TTL_dic)
            {
                foreach (var innerkvp in kvp.Value.OrderBy(x => x.Key))
                {
                    var precision = innerkvp.Key;
                    var throughput = innerkvp.Value.MaxBy(x => x.Key).Value.SizeDynamic;

                    if (!values.ContainsKey(precision)) values.Add(precision, new List<double>());

                    values[precision].Add(throughput);
                }
            }

            var report = new StringBuilder();
            report.AppendLine(
                "Precision, NumTraces, Min(%), Max, Median, Average, Stdev, ci95min, ci95max, 25thpercentile, 75thpercentile, HLL_standardError(%)");
            foreach (var kvp in values.OrderBy(x => x.Key))
                report.AppendLine(GETSTATLINE(kvp.Key, kvp.Value));

            var outputFilePath = Path.Combine(inputDirectory, "wss-hll-ttl-dynamicMemory.csv");
            File.WriteAllText(outputFilePath, report.ToString());
            Console.WriteLine($"[AE {DateTime.Now}] WSS File Outputted:\n" +
                              $"\tFILE PATH: [{outputFilePath}]\n" +
                              $"\tFILE SHA-1: [{HashingHelper.ComputeSHA1Hash(outputFilePath)}]");
        }
    }
    #endregion RESULTS ANALYSIS
}