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
using TTLsMatter.Datasets.Common.Entities;
using TTLsMatter.WSS.Common;

namespace TTLsMatter.WSS.Exact;


[Serializable]
public class WssCalculatorOptimized : IWssCalculator
{
    public class WssCalculatorEntry
    {
        public WssCalculatorEntry(uint et, uint bs)
        {
            EvictionTime = et;
            BlockSize = bs;
        }

        public uint EvictionTime;
        public uint BlockSize;
    }

    /// <summary>
    /// DO NOT USE THIS VARIABLE UNLESS YOU KNOW WHAT YOU ARE DOING
    /// </summary>
    public bool DISABLE_EVICTION = false;


    public static int MAXITEMS = 1000 * 1000 * 1000; /*To avoid going out of memory*/

    #region CTOR

    public double AvgBlockSize = 0;
    private long NumInsertedItems = 0;

    /// <summary>
    /// Stores the key and the corresponding block size
    /// </summary>
    private Dictionary<ulong, WssCalculatorEntry> _dic;


    private bool _isTTL;
    private int _fixedBlockSize;

    public WssCalculatorOptimized(bool isTtl, int fixedBlockSize)
    {
        _isTTL = isTtl;
        _fixedBlockSize = fixedBlockSize;

        _dic = new(MAXITEMS/100+1);
    }

    #endregion

    #region TTL methods
    /// <summary>
    /// The underlying assumption is that we dont go back in time.
    /// So AddRequests accepts monotonically increasing timestamps
    ///
    /// If you can go back in time where AddRequest accepts non-ordered timestamps
    /// then you need to update this implementation (TODO: easy with a dirty flag)
    /// </summary>
    private uint lastTimeEvicted = 0;
    private void Evict(uint currentTime)
    {
        if (DISABLE_EVICTION) return;
        if (lastTimeEvicted >= currentTime) return;
        lastTimeEvicted = currentTime;
        foreach (var kvp in _dic.Where(x => x.Value.EvictionTime <= currentTime))
        {
            _dic.Remove(kvp.Key);
        }
    }

    #endregion

    #region INTERFACE MEMBERS

    public long NumberOfInserts { get; set; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddRequest(Request request)
    {
        if (_dic.Count > MAXITEMS)
            return;

        AvgBlockSize = ((request.BlockSize - AvgBlockSize) / ++NumInsertedItems) + AvgBlockSize;
        NumberOfInserts++;
        if (_dic.TryGetValue(request.KeyHash, out var tempEntry))
        {
            //update ttl if needed (note that the eviction time should be set in the request)
            if (request.EvictionTime > tempEntry.EvictionTime)
                _dic[request.KeyHash].EvictionTime = request.EvictionTime;
        }
        else
        {
            /*add since it does not exist*/
            _dic.Add(request.KeyHash, new WssCalculatorEntry(request.EvictionTime, request.BlockSize));
        }
    }

    public void AddRequests(Request[] requests, int startIdx, int endIdxExclusive)
    {
        for (int i = startIdx; i < endIdxExclusive; i++) AddRequest(requests[i]);
    }

    public long GetWss_FixedBlockSize(uint currentTime)
    {
        if (_isTTL)
            Evict(currentTime);

        return (long)_dic.Count * _fixedBlockSize;
    }

    public long GetWss_VariableBlockSize(uint currentTime)
    {
        if (_isTTL)
            Evict(currentTime);
        return (long)_dic.Sum(x => (long)x.Value.BlockSize);
    }

    public long GetWss_VariableBlockSize_RunningAverage(uint currentTime)
    {
        if (_isTTL)
            Evict(currentTime);

        return (long)(_dic.Count *AvgBlockSize);
    }

    public long GetCardinality(uint currentTime)
    {
        if (_isTTL)
            Evict(currentTime);

        return _dic.Count;
    }

    public void Clean()
    {
        _dic.Clear();
    }

    public byte[] SerializeStatic()
    {
        return null;
    }

    public byte[] SerializeDynamic()
    {
        return null;
    }

    public int Deserialize(byte[] serializedObject)
    {
        return 1;
    }

    public void Merge(IWssCalculator other)
    {
        throw new NotImplementedException();
    }

    #endregion
}