﻿/*
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
using TTLsMatter.Datasets.Common;
using TTLsMatter.Datasets.Common.Entities;
using TTLsMatter.MRC.Common;
using TTLsMatter.MRC.Tools;

namespace TTLsMatter.MRC.Olken;

/// <summary>
/// (c) Sari Sultan
/// </summary>
public class Olken : IMrcGenerationAlgorithm
{
    #region constructor and internal variables

    /// <summary>
    /// A serial number for items inserted into the tree.
    /// This is monotonically increasing.
    /// </summary>
    private long _sequenceNumber;

    /// <summary>
    /// The number of total requests processed in Shards
    /// </summary>
    public long _numberOfRequests;


    #region STACK DISTANCE HISTOGRAMS

    /// <summary>
    /// A stack distance histogram for fixed size blocks
    /// </summary>
    public long[] _histogramFixedBlockSize;

    /// <summary>
    /// A stack distance histogram for variable size blocks using moving average
    /// of the block sizes in the request.
    /// </summary>
    public long[] _histogramMovingAverage;

    /// <summary>
    /// Used to populate the <see cref="_histogramMovingAverage"/>
    /// </summary>
    private double _avgBlockSize;

    /// <summary>
    /// Used to populate the <see cref="_histogramMovingAverage"/> working with <see cref="_avgBlockSize"/>
    /// </summary>
    private long _numInsertedItems;

    #endregion STACK DISTANCE HISTOGRAMS

    /// <summary>
    /// Avl tree used in Olken
    /// </summary>
    private OlkenAvl _tree;

    /// <summary>
    /// Mapping between the keys and their details in the tree.
    /// </summary>
    private Dictionary<ulong, long> _keyToSnDic;


    /// <summary>
    /// This flag is used to limit the number of items in the Olken implementation.
    /// If the number of items maintained is exceeded then the LRU item will be kicked.
    /// It is simple to find the LRU item in the tree since its ordered by sequence number, so
    /// the one of the far left is the oldest (i.e., LRU).
    /// </summary>
    public readonly bool LimitNumberOfObjectsInOlken;

    /// <summary>
    /// Maximum number of Items in Olken
    /// </summary>
    public readonly int MaxNumberOfObjects;

    private readonly long _bucketSize;
    private readonly int _numBuckets;
    private readonly uint _fixedBlockSize;


    public Olken(long maxCacheSize, long bucketSize, uint fixedBlockSize,
        bool limitNumberOfObjectsInOlken = true,
        int maxNumOfItems = DatasetConfig.MaxNumberOfDistinctObjects)
    {
        //Olken stuff 
        _numberOfRequests = 0;
        _bucketSize = bucketSize;
        _numBuckets = (int)((maxCacheSize + bucketSize - 1) / bucketSize);
        if (_numBuckets == 0)
            throw new Exception(
                $"Number of buckets = 0 ({nameof(maxCacheSize)}: {maxCacheSize}, {nameof(bucketSize)}: {bucketSize})");
        _fixedBlockSize = fixedBlockSize;
        _histogramFixedBlockSize = new long[_numBuckets];
        _histogramMovingAverage = new long[_numBuckets];

        _tree = new OlkenAvl();
        _keyToSnDic = new Dictionary<ulong, long>();

        LimitNumberOfObjectsInOlken = limitNumberOfObjectsInOlken;
        if (LimitNumberOfObjectsInOlken)
        {
            MaxNumberOfObjects = maxNumOfItems;
        }
    }

    #endregion constructor and internal variables

    #region Interface IMrcGenerationAlgorithm Members

    public void AddRequests(Request[] requests, int startIdx, int endIdxExclusive)
    {
        for (int i = startIdx; i < endIdxExclusive; i++)
            AddRequest(requests[i]);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddRequest(Request request)
    {
        _numberOfRequests++;

        _avgBlockSize = (request.BlockSize - _avgBlockSize) / ++_numInsertedItems + _avgBlockSize;

        if (_keyToSnDic.TryGetValue(request.KeyHash, out var sn))
        {
            /*Hit*/
            _tree.GetStackDistance(sn, request.KeyHash);

            #region UPDATING STACK DISTANCE HISTOGRAMS

            if (_tree.LastChildrenCount == 0 )
                throw new Exception("Olken: Stack distance on a hit shouldn't be zero.");

            var bucketIdxFixed =
                (int)((_tree.LastChildrenCount * _fixedBlockSize + _bucketSize - 1) / _bucketSize);
            if (bucketIdxFixed < _numBuckets)
                _histogramFixedBlockSize[bucketIdxFixed]++;

            var bucketIdxFixedIncr =
                (int)((_tree.LastChildrenCount * _avgBlockSize + _bucketSize - 1) / _bucketSize);
            if (bucketIdxFixedIncr < _numBuckets)
                _histogramMovingAverage[bucketIdxFixedIncr]++;

            #endregion

            //remove the node from the tree
            _tree.Remove(sn);
            sn = ++_sequenceNumber;
            _tree.Insert(request.KeyHash, sn, request.EvictionTime, request.BlockSize);
            _keyToSnDic[request.KeyHash] = sn;
        }
        else
        {
            /*Miss*/

            //add to the avl tree
            sn = ++_sequenceNumber;
            _tree.Insert(request.KeyHash, sn, request.EvictionTime, request.BlockSize);

            //add to the global dictionary
            _keyToSnDic.Add(request.KeyHash, sn);


            //make sure we did not go through the Olken Size Limit
            if (LimitNumberOfObjectsInOlken)
            {
                while (_keyToSnDic.Count > MaxNumberOfObjects)
                {
                    var originalNodeCount = _tree.NodeCount;
                    var lruKey = _tree.RemoveLruItem();
                    if (originalNodeCount == _tree.NodeCount)
                        throw new Exception("Olken: Impossible case [originalNodeCount]");

                    _keyToSnDic.Remove(lruKey);
                }
            }
        }
    }


    public string GetMrc_FixedBlockSize()
    {
        return MrcConstructor.GetMrcFromHistogram(_histogramFixedBlockSize, _numberOfRequests, _bucketSize);
    }

    public string GetMrc_VariableBlockSize_RunningAverage()
    {
        return MrcConstructor.GetMrcFromHistogram(_histogramMovingAverage, _numberOfRequests, _bucketSize);
    }

    #endregion Interface IMrcGenerationAlgorithm Members
}
