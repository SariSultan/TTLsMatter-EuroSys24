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
using System.Runtime.CompilerServices;
using TTLsMatter.Common.Hashing;
using TTLsMatter.Datasets.Common;
using TTLsMatter.Datasets.Common.Contracts;
using TTLsMatter.Datasets.Common.Entities;
using TTLsMatter.Datasets.Common.StreamReader;
using TTLsMatter.Datasets.Common.Types;

namespace TTLsMatter.Datasets.Twitter;

/// <summary>
/// Reader for Twitter Traces (this is a custom format, not an exact copy of the original traces).
///
/// Using the raw Twitter traces (CSV or Binary), following is the procedure to get it:
///
/// 1. FOREACH_1 c_i IN clusters
/// 2.      S = A set of all requests that are not GET (i.e., ADD, SET, CAS, etc) in cluster c_i
///             such that, each of them have
///             a. value size larger than 0
///             b. key size larger than 0
///             c. TTL larger than 0
/// 
/// 3.      G = A set of all GET requests in cluster c_i
/// 4.      FOREACH_2 g in G                    /* iterate over all GET requests in the trace */
/// 5.              IF ( S contains g.key ) then
///                          /* UINT TIME(S)    | ULONG HASH | UINT BLOCK SIZE   | UNIT EVICTION TIME (S) */ [TOTAL: 20 bytes per entry]
/// 6.                  OUTPUT  g.time          | g.keyHash  | s.value+s.key     | s.eviction_time (i.e., timestamp + TTL)
///                                             [note: if S is expired, then S new eviction time is g.time + s.TTL]
/// 7.              ENDIF
/// 8.      END FOREACH_2
/// 9. END FOREACH_1
///
/// </summary>
public class FilteredTwitterTracesReader : IBinaryDatasetReader
{
    private readonly FileStream _reader;
    private byte[] _buffer;
    private object _appDomainMutex;
    public Stopwatch TotalStopwatch { get; }
    public Stopwatch IoStopwatch { get; }

    public FilteredTwitterTracesReader(string binaryFilePath
        , object appDomainMutex
        , Stopwatch ioStopwatch
        , Stopwatch totalStopwatch
        , int batchSize = 1000 * 1000)
    {
        BinaryFormattedRequestSize = 20;
        BatchSize = batchSize;
        _appDomainMutex = appDomainMutex ?? new();
        IoStopwatch = ioStopwatch ?? new();
        TotalStopwatch = totalStopwatch ?? new();

        /*If we exceed this batch size then we cannot allocate the buffer byte array (C# has max limit of int.max)*/
        var maxBatchSize = (uint)((long)int.MaxValue - 1000000) / BinaryFormattedRequestSize;
        if (BatchSize > maxBatchSize)
            throw new Exception(
                $"{nameof(FilteredTwitterTracesReader)}: Very large batch size provided ({BatchSize}), while maximum allowed is ({maxBatchSize}).");

        _reader = File.Open(binaryFilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        _buffer = new byte[BinaryFormattedRequestSize * BatchSize];
        if (_reader.Length % BinaryFormattedRequestSize != 0)
            throw new Exception(
                $"{nameof(FilteredTwitterTracesReader)}: The length of the binary file ({_reader.Length}) should be divisible by entry size ({BinaryFormattedRequestSize}).");

        /*Initialize requests*/
        Requests = new Request[batchSize];
        for (int i = 0; i < BatchSize; i++)
            Requests[i] = new Request();
    }

    #region interface members

    public int BinaryFormattedRequestSize { get; }
    public Request[] Requests { get; set; }
    public int BatchSize { get; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ParseRequest(Request r, byte[] bytes, int index)
    {
        var idx = index;
        r.Timestamp = BitConverter.ToUInt32(bytes, idx);
        idx += 4;
        r.KeyHash = HashingHelper.MurmurHash264A(BitConverter.ToUInt64(bytes, idx));
        idx += 8;
        r.ValueSize = BitConverter.ToUInt32(bytes, idx);
        idx += 4;
        
        r.EvictionTime = BitConverter.ToUInt32(bytes, idx);
     
        if (r.ValueSize > DatasetConfig.MaxBlockSize) r.ValueSize = DatasetConfig.MaxBlockSize;
        if (r.ValueSize < DatasetConfig.MinBlockSize) r.ValueSize = DatasetConfig.MinBlockSize;
        
        r.Type = RequestType.Get; /*This filtered Twitter dataset only contains GET requests*/
    }

    public int GetBatch()
    {
        int items = 0;

        items = FileStreamHelper.ReadChunk(_reader, BatchSize, BinaryFormattedRequestSize, _buffer, IoStopwatch,
            TotalStopwatch, _appDomainMutex);

        if (items == -1) return -1; //this means that the file has ended

        for (int i = 0; i < items; i++)
            ParseRequest(Requests[i], _buffer, i * BinaryFormattedRequestSize);

        return items;
    }

    public long GetNumberOfRequests()
    {
        return _reader.Length / BinaryFormattedRequestSize;
    }


    public (Request, Request) GetFirstAndLastRequest(string binaryFileName)
    {
        var fs = File.Open(binaryFileName, FileMode.Open, FileAccess.Read, FileShare.Read);
        if (_reader.Length == 0)
        {
            var r1 = new Request() { Timestamp = 0 };
            return (r1, r1);
        }

        if (_reader.Length % BinaryFormattedRequestSize != 0)
            throw new Exception(
                $"{nameof(FilteredTwitterTracesReader)}: The length of the binary file ({_reader.Length}) should be divisible by entry size ({BinaryFormattedRequestSize}).");

        Request rf = new(), rl = new();
        byte[] bf = new byte[BinaryFormattedRequestSize], bl = new byte[BinaryFormattedRequestSize];

        var nBytesRead = fs.Read(bf, 0, BinaryFormattedRequestSize);
        if (nBytesRead != BinaryFormattedRequestSize)
            throw new Exception($"Should have read {BinaryFormattedRequestSize} [{binaryFileName}]");
        ParseRequest(rf, bf, 0);

        fs.Seek(fs.Length - BinaryFormattedRequestSize, SeekOrigin.Begin);
        nBytesRead = fs.Read(bl, 0, BinaryFormattedRequestSize);
        if (nBytesRead != BinaryFormattedRequestSize)
            throw new Exception($"Should have read {BinaryFormattedRequestSize} [{binaryFileName}]");
        ParseRequest(rl, bl, 0);

        return (rf, rl);
    }


    public void Dispose()
    {
        _reader.Dispose();
        _buffer = null;
        Requests = null;
        IoStopwatch.Stop();
        TotalStopwatch.Stop();
    }

    #endregion
}