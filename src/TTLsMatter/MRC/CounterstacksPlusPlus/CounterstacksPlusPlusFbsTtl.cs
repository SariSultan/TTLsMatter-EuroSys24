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

using System.Runtime.CompilerServices;
using MoreComplexDataStructures;
using TTLsMatter.Datasets.Common.Entities;
using TTLsMatter.HyperLogLog;
using TTLsMatter.MRC.Common;
using TTLsMatter.MRC.Tools;

namespace TTLsMatter.MRC.CounterstacksPlusPlus;

/// <summary>
/// CounterStacks++ (TTL support)
///
/// (c) Sari Sultan
/// </summary>
[Serializable]
public class CounterstacksPlusPlusFbsTtl : IMrcGenerationAlgorithm
{
    public readonly int NumberOfCounters;

    #region TTL

    public const int EvictListSizeLimit = 8 * 1000; /*As stated in the paper*/
    public uint EvictionTimeRoundingBase;
    private int _evictionMaxCapacity;
    private HashSet<uint> _evictionHashset;
    private MinHeap<uint> _evictionHeap;
    public int EvictCount { get; private set; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void EvictionRemoveMin()
    {
        var min = _evictionHeap.ExtractMin();
        _evictionHashset.Remove(min);
        EvictCount--;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public uint EvictionMin()
    {
        if (EvictCount == 0) return uint.MaxValue;
        return _evictionHeap.Peek();
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Insert(uint evictionTime)
    {
        if (evictionTime == 0) return;
        if (_evictionHashset.Contains(evictionTime)) return;

        if (EvictCount >= _evictionMaxCapacity)
        {
            var toInsert = _evictionHashset
                .OrderBy(x => x)
                .Take(EvictListSizeLimit - (EvictListSizeLimit / 10))
                .ToList();
            _evictionHashset.Clear();
            _evictionHeap.Clear();

            foreach (var u in toInsert)
            {
                _evictionHeap.Insert(u);
                _evictionHashset.Add(u);
            }

            EvictCount -= EvictListSizeLimit / 10;
            Insert(evictionTime);
        }
        else
        {
            _evictionHashset.Add(evictionTime);
            _evictionHeap.Insert(evictionTime);
            EvictCount++;
        }
    }

    public void Evict(uint et)
    {
        if (UtilizedCounters <= 1) return;

        Parallel.For(0, UtilizedCounters
            , PO
            , i =>
            {
                /*DO NOT EVICT THE LAST COUNTER*/
                if (_counters[i] == _lastHll) throw new Exception("CS++: impossible case [must be a BUG]");

                _counters[i].EvictExpiredBucketsAndCount(et);
            });

        /*SOME OF THE COUNTERS MIGHT BECOME 0 AFTER EVICTION, HENCE 0 VALUE COUNTERS ARE REMOVED*/
        var smallestIndexToRemove = int.MaxValue;
        var toRemoveCount = 0;
        for (int i = UtilizedCounters - 1; i >= 0; i--) /*-1 TO SKIP THE LAST COUNTER*/
        {
            if (_counters[i].LastCount != 0)
            {
                for (int j = i; j >= 0; j--)
                {
                    if (_counters[j].LastCount == 0)
                    {
                        Console.WriteLine("WARNING: A counter with 0 count found!");
                    }
                }

                break;
            }

            if (i < smallestIndexToRemove)
            {
                smallestIndexToRemove = i;
                _counters[i].Clean(_degreeOfParallelism); /*TODO: it's already 0 so no need to run clean()??*/
            }

            toRemoveCount++;
        }

        /* To handle removes only switch the last utilized counter
        /* with the minimum index to be removed and update the utilized counters value
        /*/
        if (smallestIndexToRemove != int.MaxValue)
        {
            var oldLastHllIndex = UtilizedCounters;
            UtilizedCounters -= toRemoveCount;

            Prune(PruningDelta);

            /*after pruning we need to move down the lastHll*/
            var temp = _counters[UtilizedCounters];
            _counters[UtilizedCounters] = _lastHll;
            _counters[oldLastHllIndex] = temp;

            if (_counters[UtilizedCounters] != _lastHll)
            {
                throw new Exception("CS++ Exception in evict: _counters[UtilizedCounters] != _lastHll");
            }

            if (_lastHll.LastCount != 0)
            {
                throw new Exception("CS++ Exception in evict: _counters[UtilizedCounters] != 0");
            }
        }

        for (int i = 0; i < UtilizedCounters; i++)
            _countMatrixPreviousLastCol[i] = _counters[i].LastCount;

        for (int i = UtilizedCounters; i < NumberOfCounters; i++)
            _countMatrixPreviousLastCol[i] = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private uint ROUND_TIME(uint time, uint rb)
    {
        uint floor = ROUND_TIME_FLOOR(time, rb);
        uint ceiling = ROUND_TIME_CEILING(time, rb);
        return (time - floor) < (ceiling - time) ? floor : ceiling;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private uint ROUND_TIME_CEILING(uint time, uint rb)
    {
        return time / rb * rb + rb;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private uint ROUND_TIME_FLOOR(uint time, uint rb)
    {
        return time / rb * rb;
    }

    #endregion

    #region VARIABLES AND CONSTRUCTOR

    public long NumberOfProcessedRequests { get; private set; }

    /// <summary>
    /// Bounded set of counters
    /// </summary>
    private HllBasicDenseTtl_Sparce[] _counters;

    private HllBasicDenseTtl_Sparce _lastHll;
    public int UtilizedCounters;

    public int NumberOfUsedCounters => UtilizedCounters;
    public readonly byte _precision; /*The hll precision [4,16]*/

    private readonly byte
        BucketIndexShift; /*Shift needed to remove the bucket index to use the rest to get the bucket value*/

    private readonly ulong
        _bucketIndexLsBsetMask; /*todo: The mask needed to get the number of zeros from the rest of the key hash*/

    public readonly uint FixedBlockSizeBytes /*The fixed block size (CounterStacks only supports fixed block size)*/;

    /// <summary>
    /// Buckets of stack distance counts.
    /// </summary>
    private long[] _stackDistanceHistogram;

    private long[] _stackDistanceHistogramRunningAverage;
    private double AvgBlockSize = 0;
    private long NumInsertedItems = 0;

    public long MaxCacheSize; /*What is the maximum cache size on the MRC*/
    public long BucketSize; /*The sizes are grouped into buckets of size BucketSize*/
    public long HistogramSizeBytes { get; set; } /*Debug variable to know what size of the stack distance histogram*/

    /*We only need the last two columns the count matrix described in the paper*/
    private long[] _countMatrixPreviousLastCol;
    private long[] _countMatrixNewLastCol;

    /*This controls how many threads to run in parallel*/
    private int _degreeOfParallelism;

    public int Downsampling; /*Downsampling parameters used*/
    public int NrSeconds; /*Add a new counter each NrSeconds*/
    public double PruningDelta; /*The pruning parameter*/

    private long
        _nextTimestamp; /*This keeps tracking of when the next batch should be added based on number of elapsed seconds in the trace*/

    // public HllBasicDenseTtlCMR cmrHLL = new HllBasicDenseTtlCMR(12);
    private ParallelOptions PO;

    public CounterstacksPlusPlusFbsTtl(
        byte hll_p
        , int degreeOfParallelism /*Parallel threads*/
        , uint fixedBlockSize /*The assumed fixed block size*/
        , long maxCacheSize /*Maximum cache size on the MRC*/
        , long bucketSize
        , int numberOfCounters
        , uint ttlRoundingSeconds
        , bool HiFi = true
    )
    {
        if (degreeOfParallelism == 0)
            throw new Exception("Degree of parallelism cannot be 0");
        _degreeOfParallelism = degreeOfParallelism;
        PO = new ParallelOptions() { MaxDegreeOfParallelism = _degreeOfParallelism };
        _precision = hll_p;
        EvictionTimeRoundingBase = ttlRoundingSeconds;
        Downsampling = 10 * 1000;
        NrSeconds = (HiFi ? 60 : 3600);
        PruningDelta = (HiFi ? 0.02 : 0.1);
        NumberOfCounters = numberOfCounters;
        _counters = new HllBasicDenseTtl_Sparce[NumberOfCounters];
        for (var i = 0; i < NumberOfCounters; i++)
            _counters[i] = new HllBasicDenseTtl_Sparce(_precision, i);
        _lastHll = _counters[UtilizedCounters];

        BucketSize = bucketSize;
        if (maxCacheSize == 0 || maxCacheSize <= bucketSize << 2)
            throw new Exception(
                $"Incorrect maximum cache size provided [{MaxCacheSize}] (should be > {bucketSize << 2})");
        MaxCacheSize = maxCacheSize;

        FixedBlockSizeBytes = fixedBlockSize;

        BucketIndexShift = (byte)(64 - _precision);
        _bucketIndexLsBsetMask = 1UL << BucketIndexShift;

        //Count matrices initialization (should be dynamically changed when more than x counters arrive)
        _countMatrixNewLastCol = new long[NumberOfCounters + 1];
        _countMatrixPreviousLastCol = new long[NumberOfCounters + 1];


        //setting the stack distance buckets
        int numberOfBuckets = (int)(MaxCacheSize / BucketSize);
        _stackDistanceHistogram = new long[numberOfBuckets];
        _stackDistanceHistogramRunningAverage = new long[numberOfBuckets];
        HistogramSizeBytes = (long)numberOfBuckets * sizeof(long);

        _evictionHeap = new MinHeap<uint>();
        EvictCount = 0;
        _evictionHashset = new HashSet<uint>(EvictListSizeLimit + 1);
        _evictionMaxCapacity = EvictListSizeLimit;
    }

    #endregion VARIABLES


    #region COUNTERSTACKS MAIN METHODS

    long _numberOfRequestsInLastBatch; /*added to avoid subtractions*/
    private uint _currentTime;

    private long MERGE_SN = 0;

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public void ProcessStack()
    {
        MERGE_SN++;
        NumberOfProcessedRequests += _numberOfRequestsInLastBatch;

        _lastHll.Count();
        var lastHllOriginalCount = _lastHll.LastCount;
        _lastHll.EvictExpiredBucketsAndCount(_currentTime);
        var evictedInCurrentBatch = lastHllOriginalCount - _lastHll.LastCount;
        if (_degreeOfParallelism == 1)
        {
            for (int i = 0; i < UtilizedCounters; i++)
            {
                if (_counters[i] == _lastHll) throw new Exception("No need to merge here, handled above");
                _counters[i].MergeCount(_lastHll, MERGE_SN); /*should be measured with a clean counter*/
            }
        }
        else
        {
            Parallel.For(0, UtilizedCounters
                , PO
                , i =>
                {
                    if (_counters[i] == _lastHll) throw new Exception("No need to merge here, handled above");

                    _counters[i].MergeCount(_lastHll, MERGE_SN);
                });
        }

        UtilizedCounters++; /*this shifts the counters which includes the last hll as the new one*/


        if (UtilizedCounters >= _countMatrixNewLastCol.Length)
            throw new Exception(
                "CS++: impossible case, we cannot have more than the predefined constant number of counters.");

        _countMatrixNewLastCol[0] = _counters[0].LastCount;
        for (var i = 0; i < UtilizedCounters - 1; i++)
        {
            _countMatrixNewLastCol[i + 1] = _counters[i + 1].LastCount;

            var nHitsAtThisSd = (_countMatrixNewLastCol[i + 1] - _countMatrixPreviousLastCol[i + 1])
                                - (_countMatrixNewLastCol[i] - _countMatrixPreviousLastCol[i]);


            if (nHitsAtThisSd != 0) /*can include only positive and negatives */
            {
                long maxStackDistance = _countMatrixNewLastCol[i];
                long minStackDistance = _countMatrixPreviousLastCol[i + 1];
                long stackDistance = (minStackDistance == 0)
                    ? maxStackDistance
                    : (maxStackDistance + minStackDistance) >> 1;

                int bucketIndex = (int)((stackDistance * FixedBlockSizeBytes + BucketSize - 1) / BucketSize);
                if (bucketIndex < _stackDistanceHistogram.Length)
                    _stackDistanceHistogram[bucketIndex] += nHitsAtThisSd;

                int bucketIndexIncr = (int)((stackDistance * AvgBlockSize + BucketSize - 1) / BucketSize);
                if (bucketIndexIncr < _stackDistanceHistogramRunningAverage.Length)
                    _stackDistanceHistogramRunningAverage[bucketIndexIncr] += nHitsAtThisSd;
            }
        }

        //update stack distance for the last row
        long nHitsLastCounterSd = _numberOfRequestsInLastBatch - (long)evictedInCurrentBatch -
                                  _countMatrixNewLastCol[UtilizedCounters - 1];

        if (nHitsLastCounterSd != 0)
        {
            long stackDistance =
                _countMatrixNewLastCol[UtilizedCounters] / 2; /* because it could be from 0 up to that size*/
            if (stackDistance == 0) stackDistance = 1;

            int bucketIndexLastRow = (int)((stackDistance * FixedBlockSizeBytes + BucketSize - 1) / BucketSize);
            if (bucketIndexLastRow < _stackDistanceHistogram.Length)
                _stackDistanceHistogram[bucketIndexLastRow] += nHitsLastCounterSd;

            int bucketIndexLastRowIncr = (int)((stackDistance * AvgBlockSize + BucketSize - 1) / BucketSize);
            if (bucketIndexLastRowIncr < _stackDistanceHistogramRunningAverage.Length)
                _stackDistanceHistogramRunningAverage[bucketIndexLastRowIncr] += nHitsLastCounterSd;
        }

        //Make the last column from the count matrix as the previous one
        Array.Copy(_countMatrixNewLastCol, 0, _countMatrixPreviousLastCol, 0, UtilizedCounters);


        //Pruning
        Prune(PruningDelta);

        if (UtilizedCounters >= NumberOfCounters) /*cannot be greater than*/
        {
            var oldNumUsedCounters = UtilizedCounters;
            PruneClosest();
            if (oldNumUsedCounters == UtilizedCounters)
            {
                throw new Exception("Algorithm fails, not enough counters.");
            }
        }

        _numberOfRequestsInLastBatch = 0;

        //Console.WriteLine($"Setting lastHll to index: {UtilizedCounters}");
        _lastHll = _counters[UtilizedCounters];

        if (_lastHll != _counters[UtilizedCounters])
            throw new Exception("EXCEPTION _lastHll != _counters[UtilizedCounters ]");

        if (_lastHll.LastCount != 0)
            throw new Exception("EXCEPTION _lastHll.LastCount != 0");

        if (UtilizedCounters > 1)
        {
            WORKING_SET_SIZE = (long)(_counters[0].LastCount * AvgBlockSize);
        }

        UpdateDownsampling();
    }

    private long WORKING_SET_SIZE;

    private const long GB = 1L * 1024 * 1024 * 1024;
    private const int downsampling_base = 10000;
    private const int downsampling_max = 1000000;
    private const int downsampling_min = downsampling_base;

    private void UpdateDownsampling()
    {
        var sizeInGb = (int)(WORKING_SET_SIZE / GB);
        Downsampling = sizeInGb * downsampling_base;
        if (Downsampling < downsampling_min) Downsampling = downsampling_min;
        if (Downsampling > downsampling_max) Downsampling = downsampling_max;
    }

    void Prune(double pruningDelta)
    {
        if (pruningDelta == 0.000000000000000) return;
        if (UtilizedCounters <= 1) return;
        var deletedItems = 0;
        {
            var factor = 1 - pruningDelta;
            var lastUpdatedLocation = 0;
            for (var idx = 1; idx < UtilizedCounters; idx++)
            {
                if (_counters[idx].LastCount >= (long)Math.Floor(factor * _counters[lastUpdatedLocation].LastCount))
                {
                    //keep the old counter
                    deletedItems++;
                }
                else
                {
                    lastUpdatedLocation++;
                    if (lastUpdatedLocation == idx) continue;
                    _counters[lastUpdatedLocation].LastCount = _counters[idx].LastCount;
                    //swap
                    var oldHll = _counters[lastUpdatedLocation];
                    _counters[lastUpdatedLocation] = _counters[idx];
                    _counters[idx] = oldHll;
                    _countMatrixPreviousLastCol[lastUpdatedLocation] = _countMatrixPreviousLastCol[idx];
                }
            }
        }

        int newNumItems = UtilizedCounters - deletedItems;
        for (var i = newNumItems; i < UtilizedCounters; i++)
        {
            _counters[i].LastCount = 0; /*todo: not needed as it is in clean*/
            _countMatrixPreviousLastCol[i] = 0;

            _counters[i].Clean(_degreeOfParallelism);
        }

        UtilizedCounters = newNumItems;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PruneClosest()
    {
        //find closest factor
        double smallestFactor = 1;
        for (var i = 1; i < UtilizedCounters; i++)
        {
            var diff = 1.0 - (_counters[i].LastCount * 1.0 / _counters[i - 1].LastCount);
            if (diff < smallestFactor && diff != 0) smallestFactor = Math.Ceiling(diff * 1000) / 1000;
        }

        //prune those items only
        Prune(smallestFactor);
    }

    #endregion COUNTERSTACKS MAIN METHODS


    #region INTERFACE MEMBERS

    public void AddRequest(Request request)
    {
        throw new NotImplementedException();
    }


    private uint _lastEvictionTime = 0;

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public void AddRequests(Request[] requests, int startIdx, int endIdxExclusive)
    {
        int numOfRequests = endIdxExclusive - startIdx;
        if (numOfRequests <= 0) throw new Exception($"Error: Number of requests cannot be {numOfRequests}");

        if (_nextTimestamp == 0) _nextTimestamp = requests[0].Timestamp + NrSeconds;

        for (int i = startIdx; i < endIdxExclusive; i++)
        {
            AvgBlockSize = ((requests[i].BlockSize - AvgBlockSize) / ++NumInsertedItems) + AvgBlockSize;

            #region TTL

            uint evictionTime = ROUND_TIME(requests[i].EvictionTime, EvictionTimeRoundingBase);

            if (EvictionMin() <= requests[i].Timestamp)
            {
                uint toEvictTs = EvictionMin();
                EvictionRemoveMin();
                while (EvictionMin() <= requests[i].Timestamp)
                {
                    toEvictTs = EvictionMin();
                    EvictionRemoveMin();
                    if (EvictCount == 0) break;
                }

                _lastEvictionTime = toEvictTs;
                if (toEvictTs != 0)
                {
                    Evict(toEvictTs); //there will be an eviction inside process stack
                    _currentTime = (i == 0) ? _currentTime : requests[i - 1].Timestamp;
                    ProcessStack();
                    _numberOfRequestsInLastBatch = 0;
                    _nextTimestamp = requests[i].Timestamp + NrSeconds;

                    if (evictionTime > _lastEvictionTime)
                        Insert(evictionTime);
                    continue;
                }
            }

            /*Add request to last hll*/
            _numberOfRequestsInLastBatch++;
            _lastHll.AddHash(requests[i].KeyHash, evictionTime);
            if (evictionTime > _lastEvictionTime)
                Insert(evictionTime);

            #endregion TTL

            if (_numberOfRequestsInLastBatch > Downsampling)
            {
                _currentTime = (i == 0) ? _currentTime : requests[i - 1].Timestamp;

                /*Evictions here does not make sense*/
                ProcessStack();
                _numberOfRequestsInLastBatch = 0;
                _nextTimestamp = requests[i].Timestamp + NrSeconds;
            }
            else if (requests[i].Timestamp >= _nextTimestamp)
            {
                _currentTime = (i == 0) ? _currentTime : requests[i - 1].Timestamp;
                /*Evictions here does not make sense*/
                ProcessStack();
                _numberOfRequestsInLastBatch = 0;
                _nextTimestamp = requests[i].Timestamp + NrSeconds;
            }
        }

        _currentTime = requests[endIdxExclusive - 1].Timestamp;
    }

    public string GetMrc_FixedBlockSize()
    {
        if (_numberOfRequestsInLastBatch != 0)
        {
            ProcessStack(); //it will reset the number of requests in the last batch
        }

        return MrcConstructor.GetMrcFromHistogram(_stackDistanceHistogram, NumberOfProcessedRequests, BucketSize);
    }

    public string GetMrc_VariableBlockSize()
    {
        return null;
    }

    public string GetMrc_VariableBlockSize_RunningAverage()
    {
        if (_numberOfRequestsInLastBatch != 0)
        {
            ProcessStack(); //it will reset the number of requests in the last batch
        }

        return MrcConstructor.GetMrcFromHistogram(_stackDistanceHistogramRunningAverage, NumberOfProcessedRequests,
            BucketSize);
    }

    #endregion
}