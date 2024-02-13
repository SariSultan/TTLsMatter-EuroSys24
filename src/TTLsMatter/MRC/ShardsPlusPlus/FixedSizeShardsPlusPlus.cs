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
using Priority_Queue;
using TTLsMatter.Datasets.Common;
using TTLsMatter.Datasets.Common.Entities;
using TTLsMatter.MRC.Common;
using TTLsMatter.MRC.Olken;
using TTLsMatter.MRC.Tools;

namespace TTLsMatter.MRC.ShardsPlusPlus;

/// <summary>
/// 
/// Fixed Size Shards++ MRC Generation Algorithm, Sari Sultan 
/// 
/// Summary: Fixed Size Shards (Waldspurger et al.) MRC Generation Algorithm - extended to support Time To Live (TTL)
///
///
/// (c) Sari Sultan
/// </summary>
public class FixedSizeShardsPlusPlus : IMrcGenerationAlgorithm
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
    private long _numberOfRequests;


    /// <summary>
    /// Avl tree used in Olken
    /// </summary>
    public OlkenAvl _tree;

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


    #region SHARDS Stuff

    public double R;

    private long T;
    private readonly long P;

    /*Shards use bitwise mask instead of mod. (won't make a big difference)*/
    private readonly long PMask;
    private readonly bool _isShardsAdjMode;


    #region Fixed Size stuff

    public class ShardsPlusPlusPqNode : GenericPriorityQueueNode<long>
    {
        public ulong KeyHash;

        /// <summary>
        /// This can be moved to the <see cref="FixedSizeShardsPlusPlus._keyToSnDic"/>
        /// However, I don't think it will affect performance.
        /// </summary>
        public ShardsPlusPlusEtNode EtNode;
    }

    public class ShardsPlusPlusEtNode : GenericPriorityQueueNode<uint>
    {
        public ulong KeyHash;

        /// <summary>
        /// This can be moved to the <see cref="FixedSizeShardsPlusPlus._keyToSnDic"/>
        /// However, I don't think it will affect performance.
        /// </summary>
        public ShardsPlusPlusPqNode PQNode;
    }

    private int SMax;

    /// <summary>
    /// The sampled set of objects.
    ///
    /// Priority: T when the object was sampled
    /// The implementation used dequeue the minimal priority
    /// Hence, when adding elements the priority should be P-T.
    /// Since T=R*P, T is always less or equal to P.
    /// Hence, a high threshold means high sampling rate since P is fixed.
    /// Thus, when enqueuing to the priority queue the complement is used as priority
    /// When dequeued I convert it back. 
    /// </summary>
    [NonSerialized] public GenericPriorityQueue<ShardsPlusPlusPqNode, long> _sampledSet;

    /// <summary>
    /// Priority is the eviction time (converted from uint to long) 
    /// </summary>
    [NonSerialized] public GenericPriorityQueue<ShardsPlusPlusEtNode, uint> _evictionSet;

    private Stack<ShardsPlusPlusPqNode> _recycledSampledQueueNodes;
    private Stack<ShardsPlusPlusEtNode> _recycledEvictionQueueNodes;

    #region STACK DISTANCE HISTOGRAMS

    /// <summary>
    /// A stack distance histogram for fixed size blocks
    /// </summary>
    private double[,] _histogramFixedBlockSize;

    /// <summary>
    /// A stack distance histogram for variable size blocks using moving average
    /// of the block sizes in the request.
    /// </summary>
    private double[,] _histogramMovingAverage;

    /// <summary>
    /// Used to populate the <see cref="_histogramMovingAverage"/>
    /// </summary>
    private double _avgBlockSize;

    /// <summary>
    /// Used to populate the <see cref="_histogramMovingAverage"/> working with <see cref="_avgBlockSize"/>
    /// </summary>
    public long _numInsertedItems;

    #endregion STACK DISTANCE HISTOGRAMS

    #endregion Fixed Size stuff

    #endregion SHARDS Stuff

    public FixedSizeShardsPlusPlus(int sMax, bool isShardsAdj,
        long maxCacheSize, long bucketSize, uint fixedBlockSize,
        bool limitNumberOfObjectsInOlken = true, int maxNumOfItems = DatasetConfig.MaxNumberOfDistinctObjects)
    {
        //Shards stuff
        _isShardsAdjMode = isShardsAdj;

        P = 1 << 24 /*(value from paper)should be a power of two when using bitwise mask*/;
        PMask = P - 1;

        R = 0.1;
        /* R = T / P
         * T = RP
         * FAST'15 "For a given sampling rate R, the threshold Tis set to round(R·P)
         */
        T = (long)Math.Round(R * P);

        //Olken stuff 
        _numberOfRequests = 0;
        _bucketSize = bucketSize;
        _numBuckets = (int)((maxCacheSize + bucketSize - 1) / bucketSize);
        if (_numBuckets == 0)
            throw new Exception(
                $"Number of buckets = 0 ({nameof(maxCacheSize)}: {maxCacheSize}, {nameof(bucketSize)}: {bucketSize})");
        _fixedBlockSize = fixedBlockSize;
        _histogramFixedBlockSize = new double[_numBuckets, 2];
        _histogramMovingAverage = new double[_numBuckets, 2];

        _tree = new OlkenAvl();
        _keyToSnDic = new Dictionary<ulong, long>();

        LimitNumberOfObjectsInOlken = limitNumberOfObjectsInOlken;
        if (LimitNumberOfObjectsInOlken)
        {
            MaxNumberOfObjects = maxNumOfItems;
            if (sMax > maxNumOfItems) throw new Exception("Cannot have a maximum number of objects less than sMax");
        }


        SMax = sMax;
        _sampledSet = new GenericPriorityQueue<ShardsPlusPlusPqNode, long>(sMax + 1);
        _recycledSampledQueueNodes = new Stack<ShardsPlusPlusPqNode>(sMax + 1);
        _evictionSet = new GenericPriorityQueue<ShardsPlusPlusEtNode, uint>(sMax + 1);
        _recycledEvictionQueueNodes = new Stack<ShardsPlusPlusEtNode>(sMax + 1);
    }

    #endregion constructor and internal variables

    #region Interface IMrcGenerationAlgorithm Members

    public void AddRequests(Request[] requests, int startIdx, int endIdxExclusive)
    {
        for (int i = startIdx; i < endIdxExclusive; i++)
            AddRequest(requests[i]);
    }

    private double _stackDistanceFixed;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddRequest(Request request)
    {
        _numberOfRequests++;
        
        _avgBlockSize = (request.BlockSize - _avgBlockSize) / ++_numInsertedItems + _avgBlockSize; /*bug: fixed aug 14, 2023 (it should be here and not below)*/

        var Ti = (long)request.KeyHash & PMask;
        if (Ti >= T) return;

        HandleEvictions(request.Timestamp);


        _stackDistanceFixed = 0;

        if (_keyToSnDic.TryGetValue(request.KeyHash, out var sn))
        {
            /*Hit*/
            _tree.GetStackDistance(sn, request.KeyHash);

            if (_tree.LastChildrenCount == 0 )
                throw new Exception("Shouldn't be zero here");

            _stackDistanceFixed = _tree.LastChildrenCount / R;

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

            //=============================
            /*
                * and its associated threshold value, Ti = hash(Li) mod P
                */
            ShardsPlusPlusPqNode pqNode;
            ShardsPlusPlusEtNode etNode;

            if (_recycledSampledQueueNodes.Any())
            {
                pqNode = _recycledSampledQueueNodes.Pop();
                pqNode.KeyHash = request.KeyHash;
            }
            else
            {
                pqNode = new ShardsPlusPlusPqNode() { KeyHash = request.KeyHash };
            }

            if (_recycledEvictionQueueNodes.Any())
            {
                etNode = _recycledEvictionQueueNodes.Pop();
                etNode.KeyHash = request.KeyHash;
                etNode.PQNode = pqNode;
            }
            else
            {
                etNode = new ShardsPlusPlusEtNode() { KeyHash = request.KeyHash, PQNode = pqNode };
            }

            pqNode.EtNode = etNode;
            _sampledSet.Enqueue(pqNode, P - Ti);
            _evictionSet.Enqueue(etNode, request.EvictionTime);

            if (_sampledSet.Count > SMax)
            {
                long tmin;
                do
                {
                    var poppedpqNode = _sampledSet.Dequeue();
                    tmin = poppedpqNode.Priority;
                    var popedKey = poppedpqNode.KeyHash;

                    /*This item should also be removed from the eviction time set*/
                    _evictionSet.Remove(poppedpqNode.EtNode);

                    /*Optimization to relax memory pressure*/
                    _recycledSampledQueueNodes.Push(poppedpqNode);
                    _recycledEvictionQueueNodes.Push(poppedpqNode.EtNode);

                    /*Tmin=P-Tmax => P-(P-Tmax) = Tmax (conversion because its a min PQ, see comment in the decleration)*/
                    T = P - tmin;

                    R = (double)T / P;

                    //Remove from Olken
                    var popedSn = _keyToSnDic[popedKey];
                    _keyToSnDic.Remove(popedKey);
                    _tree.Remove(popedSn);

                    if (_sampledSet.Count == 0)
                    {
                        if (_evictionSet.Count != 0)
                            throw new Exception(
                                $"{nameof(FixedSizeShardsPlusPlus)} Number of nodes in {nameof(_evictionSet)} should be 0 ");
                        break;
                    }
                } while (_sampledSet.First.Priority == tmin);
            }


            if (_sampledSet.Count > SMax)
                throw new Exception(
                    $"{nameof(FixedSizeShardsPlusPlus)} Number of nodes in {nameof(_sampledSet)} exceeds {nameof(SMax)}");

            if (_tree.NodeCount > SMax)
                throw new Exception(
                    $"{nameof(FixedSizeShardsPlusPlus)} Number of nodes in {nameof(Olken.Olken)} exceeds {nameof(SMax)}");

            if (_evictionSet.Count > SMax)
                throw new Exception(
                    $"{nameof(FixedSizeShardsPlusPlus)} Number of nodes in {nameof(_evictionSet)} exceeds {nameof(SMax)}");

            if (_sampledSet.Count != _tree.NodeCount)
                throw new Exception(
                    $"{nameof(FixedSizeShardsPlusPlus)} Number of nodes mismatch between {nameof(_evictionSet)} and  {nameof(Olken.Olken)}");

            if (_sampledSet.Count != _keyToSnDic.Count)
                throw new Exception(
                    $"{nameof(FixedSizeShardsPlusPlus)} Number of nodes mismatch between {nameof(_sampledSet)} and  {nameof(_keyToSnDic)}");
            //===========================

            //make sure we did not go through the Olken Size Limit
            if (LimitNumberOfObjectsInOlken)
            {
                while (_keyToSnDic.Count > MaxNumberOfObjects)
                {
                    var originalNodeCount = _tree.NodeCount;
                    var lruKey = _tree.RemoveLruItem();
                    if (originalNodeCount == _tree.NodeCount)
                        throw new Exception("Impossible case [originalNodeCount]");

                    _keyToSnDic.Remove(lruKey);

                    //IMPORTANT NOTE FOR FIXED SIZE SHARDS. THERE IS NO NEED TO REMOVE FROM SAMPLED SET BECAUSE
                    //THE ASSUMPTION IS THAT sMax is always <= MaxNumberOfObjects
                }
            }
        } //!Miss


        /*This is performed in case of a hit or a miss*/


        var bucketIdxFixed = (int)((_stackDistanceFixed * _fixedBlockSize + _bucketSize - 1) / _bucketSize);
        if (bucketIdxFixed < _numBuckets)
        {
            if (_histogramFixedBlockSize[bucketIdxFixed, 1] > T)
                _histogramFixedBlockSize[bucketIdxFixed, 0] *= T / _histogramFixedBlockSize[bucketIdxFixed, 1];

            _histogramFixedBlockSize[bucketIdxFixed, 0]++;
            _histogramFixedBlockSize[bucketIdxFixed, 1] = T;
        }
        else
        {
            /*VERY TRICKY CASE THAT CAUSES ERRORS TO SPIKE (e.g., IBM83)*/
            _histogramFixedBlockSize[0, 0]++;
            _histogramFixedBlockSize[0, 1] = T;
        }

        var bucketIdxIncr = (int)((_stackDistanceFixed * _avgBlockSize + _bucketSize - 1) / _bucketSize);
        if (bucketIdxIncr < _numBuckets)
        {
            if (_histogramMovingAverage[bucketIdxIncr, 1] > T)
                _histogramMovingAverage[bucketIdxIncr, 0] *= T / _histogramMovingAverage[bucketIdxIncr, 1];

            _histogramMovingAverage[bucketIdxIncr, 0]++;
            _histogramMovingAverage[bucketIdxIncr, 1] = T;
        }
        else
        {
            _histogramMovingAverage[0, 0]++;
            _histogramMovingAverage[0, 1] = T;
        }
    }


    public string GetMrc_FixedBlockSize()
    {
        Scale(_histogramFixedBlockSize);
        var nRequests = 0.0;
        for (int i = 0; i < _histogramFixedBlockSize.GetLength(0); i++)
            nRequests += _histogramFixedBlockSize[i, 0];

        if (_isShardsAdjMode)
        {
            var expected = R * _numberOfRequests;
            var diff = expected - nRequests;
            Console.WriteLine($"Adjusting by {diff}");
            _histogramFixedBlockSize[1, 0] += diff;
            nRequests = expected;
        }

        return MrcConstructor.GetMrcFromHistogram((_histogramFixedBlockSize), (long)nRequests, _bucketSize);
    }

    public string GetMrc_VariableBlockSize_RunningAverage()
    {
        Scale(_histogramMovingAverage);

        var nRequests = 0.0;
        for (int i = 0; i < _histogramMovingAverage.GetLength(0); i++)
            nRequests += _histogramMovingAverage[i, 0];

        if (_isShardsAdjMode)
        {
            var expected = R * _numberOfRequests;
            var diff = expected - nRequests;
            _histogramMovingAverage[1, 0] += diff;
            nRequests = expected;
        }

        return MrcConstructor.GetMrcFromHistogram((_histogramMovingAverage),
            (long)nRequests, _bucketSize);
    }

    #endregion Interface IMrcGenerationAlgorithm Members

    #region INTERNALS

    public void Scale(double[,] histogram)
    {
        for (int i = 0; i < _numBuckets; i++)
            if (histogram[i, 1] > T)
                histogram[i, 0] *= T / histogram[i, 1];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void HandleEvictions(uint currentTime)
    {
        while (_evictionSet.Count > 0)
        {
            if (_evictionSet.First.Priority <= currentTime)
            {
                var poppedEtNode = _evictionSet.Dequeue();
                var key = poppedEtNode.KeyHash;
                _sampledSet.Remove(poppedEtNode.PQNode);

                var sn = _keyToSnDic[key];
                _tree.Remove(sn);
                _keyToSnDic.Remove(key);
            }
            else break;
        }
    }

    #endregion

    
}