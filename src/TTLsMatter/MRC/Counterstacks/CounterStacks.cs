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

using TTLsMatter.Datasets.Common.Entities;
using TTLsMatter.HyperLogLog;
using TTLsMatter.MRC.Common;
using TTLsMatter.MRC.Tools;

namespace TTLsMatter.MRC.Counterstacks;

/// <summary>
/// To the best of my knowledge, there is no public implementation of CounterStacks.
/// This is the first public implementation CounterStacks.
///
/// (c) Sari Sultan 
/// </summary>
[Serializable]
public class CounterStacks : IMrcGenerationAlgorithm
{
    #region VARIABLES AND CONSTRUCTOR

    /// <summary>
    /// This tracks the high watermark of used counters
    /// </summary>
    public int MAX_NUM_COUNTERS = 0;

    /// <summary>
    /// Returns the bytes needed for the counters 
    /// </summary>
    public long MEMORY_USAGE_BASED_ON_HLLS_BYTES => MAX_NUM_COUNTERS * (1 << _precision);

    /// <summary>
    /// Tracks total number of processed requests. 
    /// </summary>
    public long NumberOfProcessedRequests { get; private set; }

    /// <summary>
    /// Bounded set of counters
    /// </summary>
    private List<HllBasicDense> _counters;

    private readonly byte _precision; /*The hll precision [4,16]*/
    private readonly int _hllNumOfBuckets; /*todo: How many buckets in each HLL*/

    private readonly byte
        _hllBucketIndexShift; /*Shift needed to remove the bucket index to use the rest to get the bucket value*/

    private readonly ulong
        _bucketIndexLsBsetMask; /*todo: The mask needed to get the number of zeros from the rest of the key hash*/

    public readonly uint FixedBlockSizeBytes /*The fixed block size (CounterStacks only supports fixed block size)*/;

    /// <summary>
    /// Buckets of stack distance counts.
    /// </summary>
    public long[] _stackDistanceHistogram;

    private long[] _stackDistanceHistogramRunningAverage;
    private double AvgBlockSize = 0;
    private long NumInsertedItems = 0;

    public long MaxCacheSize; /*What is the maximum cache size on the MRC*/
    public long BucketSize; /*The sizes are grouped into buckets of size BucketSize*/
    public long HistogramSizeBytes { get; set; } /*Debug variable to know what size of the stack distance histogram*/


    /*We only need the last two columns the count matrix described in the paper*/
    private long[] _countMatrixPreviousLastCol;
    private long[] _countMatrixNewLastCol;


    public int Downsampling; /*Downsampling parameters used*/
    public int NrSeconds; /*Add a new counter each NrSeconds*/
    public double PruningDelta; /*The pruning parameter*/

    private long
        _nextTimestamp; /*This keeps tracking of when the next batch should be added based on number of elapsed seconds in the trace*/

    public CounterStacks(
        byte precision /*HLL Prevision [4,16]*/
        , uint fixedBlockSize /*The assumed fixed block size*/
        , long maxCacheSize /*Maximum cache size on the MRC*/
        , bool isHighFidelity /*Fidelity configuration (See OSDI'14 paper)*/
        , long bucketSize = 64 * 1024 * 1024 //default 64MB
    )
    {
        if (precision < 4 || precision > 16)
        {
            throw new ArgumentException($"Precision should be in [4-16], {precision} was provided.");
        }

        if (isHighFidelity)
        {
            Downsampling = 1000 * 1000;
            NrSeconds = 60;
            PruningDelta = 0.02;
        }
        else
        {
            Downsampling = 1000 * 1000; /*low is 1M (see AET), very low is 19*/
            NrSeconds = 3600;
            PruningDelta = 0.1; /*low is 0.1 very low is 0.46*/
        }

        BucketSize = bucketSize;
        if (maxCacheSize == 0 || maxCacheSize <= bucketSize << 2)
            throw new Exception(
                $"Incorrect maximum cache size provided [{MaxCacheSize}] (should be > {bucketSize << 2})");
        MaxCacheSize = maxCacheSize;

        _precision = precision;
        FixedBlockSizeBytes = fixedBlockSize;

        _hllNumOfBuckets = 1 << precision;
        _hllBucketIndexShift = (byte)(64 - precision);
        _bucketIndexLsBsetMask = 1UL << _hllBucketIndexShift;

        //Counters initialization
        _counters = new List<HllBasicDense>() { };

        _counters.Add(new(_precision));


        //Count matrices initialization (should be dynamically changed when more than x counters arrive)
        //TODO: should be dynamic
        _countMatrixNewLastCol = new long[2000];
        _countMatrixPreviousLastCol = new long[2000];


        //setting the stack distance buckets
        int numberOfBuckets = (int)(MaxCacheSize / BucketSize);
        _stackDistanceHistogram = new long[numberOfBuckets];
        _stackDistanceHistogramRunningAverage = new long[numberOfBuckets];
        HistogramSizeBytes = (long)numberOfBuckets * sizeof(long);
    }

    #endregion VARIABLES


    private int
        _numberOfRequestsInLastBatch; /*added to avoid subtractions (this should be int because hits can be negative)*/


    private uint CurrentTime = 0;

    private void AddBatch(Request[] requests, int startIdx, int endIdx)
    {
        if (endIdx == 0) return;
        CurrentTime = requests[endIdx - 1].Timestamp;

        //Console.WriteLine($"Current timestamp: {TimeFormatHelpers.ConvertUnixTimeToDateTime(CurrentTime)}");
        foreach (var counter in _counters)
            for (int i = startIdx; i < endIdx; i++)
                counter.AddHash(requests[i].KeyHash);
    }

    public void ProcessStack()
    {
        int localDownsampling = _numberOfRequestsInLastBatch;
        _numberOfRequestsInLastBatch = 0;
        if (localDownsampling == 0) return;
        NumberOfProcessedRequests += localDownsampling;

        //  updating the hll counts 

        foreach (var counter in _counters)
            counter.Count();


        // process count matrices 
        if (_counters.Count >= _countMatrixNewLastCol.Length)
        {
            //todo:: extend the array
            throw new Exception("TODO:: extend the array sizes.");
        }

        _countMatrixNewLastCol[0] = _counters[0].LastCount;
        for (var i = 0; i < _counters.Count - 1; i++)
        {
            _countMatrixNewLastCol[i + 1] = _counters[i + 1].LastCount;

            var nHitsAtThisSd = (_countMatrixNewLastCol[i + 1] - _countMatrixPreviousLastCol[i + 1])
                                - (_countMatrixNewLastCol[i] - _countMatrixPreviousLastCol[i]);

            if (nHitsAtThisSd != 0) /*can include only positive and negatives */
            {
                long maxStackDistance = _countMatrixNewLastCol[i];
                long minStackDistance = _countMatrixPreviousLastCol[i + 1];
                long stackDistance = maxStackDistance;

                int bucketIndex = (int)((stackDistance * FixedBlockSizeBytes + BucketSize - 1) / BucketSize);
                if (bucketIndex < _stackDistanceHistogram.Length)
                    _stackDistanceHistogram[bucketIndex] += nHitsAtThisSd;

                int bucketIndexIncr = (int)((stackDistance * AvgBlockSize + BucketSize - 1) / BucketSize);
                if (bucketIndexIncr < _stackDistanceHistogramRunningAverage.Length)
                    _stackDistanceHistogramRunningAverage[bucketIndexIncr] += nHitsAtThisSd;
            }
        }

        //update stack distance for the last row
        long nHitsLastCounterSd = localDownsampling - _countMatrixNewLastCol[_counters.Count - 1];
        if (nHitsLastCounterSd != 0)
        {
            var maxSd = _countMatrixNewLastCol[_counters.Count - 1];

            int bucketIndexLastRow = (int)((maxSd * FixedBlockSizeBytes + BucketSize - 1) / BucketSize);
            if (bucketIndexLastRow < _stackDistanceHistogram.Length)
                _stackDistanceHistogram[bucketIndexLastRow] += nHitsLastCounterSd;

            int bucketIndexLastRowIncr = (int)((maxSd * AvgBlockSize + BucketSize - 1) / BucketSize);
            if (bucketIndexLastRowIncr < _stackDistanceHistogramRunningAverage.Length)
                _stackDistanceHistogramRunningAverage[bucketIndexLastRowIncr] += nHitsLastCounterSd;
        }


        //Make the last column from the count matrix as the previous one
        Array.Copy(_countMatrixNewLastCol, 0, _countMatrixPreviousLastCol, 0, _counters.Count);

        //Pruning
        Prune();


        var newHll = new HllBasicDense(_precision);
        newHll.CREATION_TIMESTAMP = CurrentTime;
        _counters.Add(newHll);


        if (_counters.Count > MAX_NUM_COUNTERS) MAX_NUM_COUNTERS = _counters.Count;
    }

    private void Prune()
    {
        if (PruningDelta > 0.00)
        {
            var nCountersBeforePruning = _counters.Count;
            var factor = 1 - PruningDelta;
            var deletedItems = 0;
            var lastUpdatedLocation = 0;
            for (var idx = 1; idx < _counters.Count; idx++)
            {
                if (_counters[idx].LastCount >= (factor * _counters[lastUpdatedLocation].LastCount))
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

            int newNumItems = _counters.Count - deletedItems;
            var indecesToRemove = new List<int>();
            for (var i = newNumItems; i < _counters.Count; i++)
            {
                _countMatrixPreviousLastCol[i] = 0;
                indecesToRemove.Add(i);
            }

            foreach (var idx in indecesToRemove.OrderByDescending(x => x).ToList())
            {
                _counters.RemoveAt(idx);
            }
        }
    }

    #region INTERFACE MEMBERS

    public void AddRequest(Request request)
    {
        throw new Exception("NOT IMPLEMENTED");
    }

    public void AddRequests(Request[] requests, int startIdx, int endIdxExclusive)
    {
        int numOfRequests = endIdxExclusive - startIdx;
        if (numOfRequests <= 0) throw new Exception($"Error: Number of requests cannot be {numOfRequests}");
        if (_nextTimestamp == 0) _nextTimestamp = requests[0].Timestamp + NrSeconds;

        int toSubmitStartIdx = startIdx;
        for (int i = startIdx; i < endIdxExclusive; i++)
        {
            AvgBlockSize = ((requests[i].BlockSize - AvgBlockSize) / ++NumInsertedItems) + AvgBlockSize;

            if (
                (requests[i].Timestamp >= _nextTimestamp &&
                 _numberOfRequestsInLastBatch > 0) /*The and is to remove periods of inactivity*/
                || _numberOfRequestsInLastBatch >= Downsampling)
            {
                AddBatch(requests, toSubmitStartIdx, i);
                toSubmitStartIdx = i;
                ProcessStack();
                _nextTimestamp = requests[i].Timestamp + NrSeconds;
            }

            _numberOfRequestsInLastBatch++;
        }

        if (toSubmitStartIdx < endIdxExclusive)
        {
            AddBatch(requests, toSubmitStartIdx, endIdxExclusive);
            /*We don't process the stack here*/
        }
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