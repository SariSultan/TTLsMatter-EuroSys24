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
using System.Runtime.Intrinsics.X86;
using TTLsMatter.Common.ByteArray;
using TTLsMatter.Common.Hashing;
using TTLsMatter.Datasets.Common;
using TTLsMatter.Datasets.Common.Entities;
using TTLsMatter.HyperLogLog;
using TTLsMatter.WSS.Common;

namespace TTLsMatter.WSS.Approximate;

[Serializable]
public class WssEstimator : IWssCalculator
{
    /// <summary>
    /// Only accessible when using FBS
    /// </summary>
    public double AvgBlockSize = 0;

    private long NumInsertedItems = 0;

    #region Serialization tests

    public static void TestSerialization()
    {
        var rand = new Random(555);
        var minBlockSize = DatasetConfig.MinBlockSize;
        var maxBlockSize = DatasetConfig.MaxBlockSize;

        var numKeys = 1000 * 1000;
        var distinctMax = 100 * 1000;

        var estimatorVbs = new WssEstimator(false, 12, 4096, 2, 8 * 1024 * 1024);
        var estimatorFbs = new WssEstimator(true, 12, 4096, 0, 0);

        for (int i = 0; i < numKeys; i++)
        {
            var key = rand.Next(0, distinctMax);
            var hash = HashingHelper.MurmurHash264A((ulong)key);
            var bs = rand.Next((int)minBlockSize, (int)maxBlockSize);

            estimatorFbs.Add(hash, (uint)bs);
            estimatorVbs.Add(hash, (uint)bs);
        }

        var estimatorSizeFbs = estimatorFbs.GetWss();

        var serializedStaticFbs = estimatorFbs.SerializeStatic();
        var serializedDynamicFbs = estimatorFbs.SerializeDynamic();

        var deserializedStaticFbs = new WssEstimator();
        deserializedStaticFbs.Deserialize(serializedStaticFbs);
        var deserializedStaticFbsWss = deserializedStaticFbs.GetWss();

        var deserializedDynamicFbs = new WssEstimator();
        deserializedDynamicFbs.Deserialize(serializedDynamicFbs);
        var deserializedDynamicFbsWss = deserializedDynamicFbs.GetWss();

        Console.WriteLine(
            $"[FBS] REF WSS: {estimatorSizeFbs} DeserializedWssStatic: {deserializedStaticFbsWss} DeserializedWssDynamic: {deserializedDynamicFbsWss}");
        Console.WriteLine(
            $"[FBS] Size Bytes Static: {serializedStaticFbs.Length:n0} Dynamic: {serializedDynamicFbs.Length:n0}");

        //============================== vbs
        var estimatorSizeVbs = estimatorVbs.GetWss();

        var serializedStaticVbs = estimatorVbs.SerializeStatic();
        var serializedDynamicVbs = estimatorVbs.SerializeDynamic();

        var deserializedStaticVbs = new WssEstimator();
        deserializedStaticVbs.Deserialize(serializedStaticVbs);
        var deserializedStaticVbsWss = deserializedStaticVbs.GetWss();

        var deserializedDynamicVbs = new WssEstimator();
        deserializedDynamicVbs.Deserialize(serializedDynamicVbs);
        var deserializedDynamicVbsWss = deserializedDynamicVbs.GetWss();

        Console.WriteLine(
            $"[VBS] REF WSS: {estimatorSizeVbs} DeserializedWssStatic: {deserializedStaticVbsWss} DeserializedWssDynamic: {deserializedDynamicVbsWss}");
        Console.WriteLine(
            $"[VBS] Size Static: {serializedStaticVbs.Length:n0} Dynamic: {serializedDynamicVbs.Length:n0}");
    }

    #endregion

    #region TOOLS //TODO:: move to common

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint NextPow2(uint val)
    {
        uint toRet = 2;
        while (toRet < val) toRet <<= 1;
        return toRet;
    }

    /// <summary>
    /// TODO: can be optimized using lzcnt because the input is a power of 2
    /// </summary>
    /// <param name="powOf2"></param>
    /// <returns></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private uint Log2(uint powOf2)
    {
        var toRetOptimized = 31 - Lzcnt.LeadingZeroCount(powOf2);
        return toRetOptimized;
    }

    #endregion


    #region INTERFACE MEMBERS

    public long NumberOfInserts { get; set; }

    public void AddRequest(Request request)
    {
        Add(request.KeyHash, request.BlockSize);
    }

    public void AddRequests(Request[] requests, int startIdx, int endIdxExclusive)
    {
        AddReqs(requests, startIdx, endIdxExclusive);
    }

    public long GetWss_FixedBlockSize(uint currentTime)
    {
        if (IsFixedBlockSize) return GetWss();
        return -1;
    }

    public long GetWss_VariableBlockSize(uint currentTime)
    {
        if (!IsFixedBlockSize) return GetWss();
        return -1;
    }

    public long GetWss_VariableBlockSize_RunningAverage(uint currentTime)
    {
        if (IsFixedBlockSize) return (long)(GetCardinality(currentTime) * AvgBlockSize);
        return -1;
    }

    public long GetCardinality(uint currentTime)
    {
        return GetCardinality();
    }

    public void Clean()
    {
        foreach (var hll in _hlls)
        {
            if (hll != null)
                hll.Clean();

            NumInsertions = 0;
            NumInsertedItems = 0;
            AvgBlockSize = 0;
        }
    }

    private byte[] SerializeInternal(bool isStatic)
    {
        var header = SerializeInternalHeader();
        if (IsFixedBlockSize)
        {
            var serializedHll = _hlls[0].Serialize();
            var arraySize = 4 + header.Length + serializedHll.Length;
            byte[] serialized = new byte[arraySize];
            var index = 0;
            ByteArrayHelper.WriteUInt32(serialized, index, (uint)arraySize);
            index += 4;
            Array.Copy(header, 0, serialized, index, header.Length);
            index += header.Length;
            Array.Copy(serializedHll, 0, serialized, index, serializedHll.Length);
            index += serializedHll.Length;
            return serialized;
        }
        else
        {
            var serializedHlls = new List<byte[]>();
            var serializedObjectSizes = 0;
            for (int i = 0; i < _hlls.Length; i++)
            {
                var so = _hlls[i].Serialize();
                serializedHlls.Add(so);
                serializedObjectSizes += so.Length;
            }

            var arraySize = 4 + header.Length + serializedObjectSizes;
            byte[] serialized = new byte[arraySize];
            var index = 0;
            ByteArrayHelper.WriteUInt32(serialized, index, (uint)arraySize);
            index += 4;
            Array.Copy(header, 0, serialized, index, header.Length);
            index += header.Length;
            for (int i = 0; i < _hlls.Length; i++)
            {
                Array.Copy(serializedHlls[i], 0, serialized, index, serializedHlls[i].Length);
                index += serializedHlls[i].Length;
            }

            return serialized;
        }
    }

    public byte[] SerializeStatic()
    {
        return SerializeInternal(true);
    }

    public byte[] SerializeDynamic()
    {
        return SerializeInternal(false);
    }

    public int Deserialize(byte[] serializedObject)
    {
        if (serializedObject == null)
            throw new Exception($"{nameof(WssEstimator)}:: Cannot deserialize from null object");
        if (serializedObject.Length <= 4)
            throw new Exception($"{nameof(WssEstimator)}:: Minimum serialized object size should be >4");

        int index = 0;
        var length = ByteArrayHelper.ReadUInt32(serializedObject, index);
        index += 4;
        if (length != serializedObject.Length)
            throw new Exception(
                $"{nameof(WssEstimator)}:: Serialized Object length should be {length} while actual length is {serializedObject.Length}");

        var header = DeserializeInternalHeader(serializedObject, ref index);
        var p = header.Item1;
        var isFbs = header.Item2;
        var fbs = header.Item3;
        var minbs = header.Item4;
        var maxbs = header.Item5;
        var numInsersions = header.Item6;

        CtorInternal(isFbs, p, fbs, minbs, maxbs, false);
        NumInsertions = numInsersions;

        for (int i = 0; i < _hlls.Length; i++)
        {
            int hllLen = (int)ByteArrayHelper.ReadUInt32(serializedObject, index);
            _hlls[i].Deserialize(serializedObject, index, index + hllLen);
            index += hllLen;
        }

        return 0;
    }

    public void Merge(IWssCalculator other)
    {
        var otherConv = (WssEstimator)other;
        NumInsertions += otherConv.NumInsertions;
        for (int i = 0; i < _numHlls; i++)
        {
            _hlls[i].MergeCount(otherConv._hlls[i]);
        }
    }

    #endregion

    /// <summary>
    /// This method returns the basic information needed for both the static and dynamic serialization
    /// </summary>
    /// <returns></returns>
    private byte[] SerializeInternalHeader()
    {
        /* TIMESTAMP IS ENCODED IN THE NAME OF THE SERIALIZED OBJECT EXTERNALLY.
         * ================================================
         * INDEX : FIELD : NUMBER OF BYTES : EXTRA COMMENTS
         * ================================================
         * 0     : Precision    : 1 BYTE : [4-16]
         * 1     : IsFbs        : 1 BYTE
         * 2     : FBS          : 4 BYTES (uint) : if IsFbs is false then this is always 0, otherwise it should be larger than 0
         * 3     : MinBlockSize : 4 BYTES (uint) : if IsFbs is true then this is always 0
         * 4     : MaxBlockSize : 4 BYTES (uint) : if IsFbs is true then this is always 0
         * 5    : NumInsersions: 8 bytes (long)
         * =================================================
         * Header total bytes: 1+1+4+4+4+8=22 BYTES
         */
        var header = new byte[22];
        ByteArrayHelper.WriteByte(header, 0, Precision);
        ByteArrayHelper.WriteByte(header, 1, IsFixedBlockSize ? (byte)1 : (byte)0);
        ByteArrayHelper.WriteUInt32(header, 2, FixedBlockSize);
        ByteArrayHelper.WriteUInt32(header, 6, MinBlockSize);
        ByteArrayHelper.WriteUInt32(header, 10, MaxBlockSize);
        ByteArrayHelper.WriteInt64(header, 14, NumInsertions);
        return header;
    }

    private (byte, bool, uint, uint, uint, long) DeserializeInternalHeader(byte[] serializedObject, ref int index)
    {
        var precision = ByteArrayHelper.ReadByte(serializedObject, index++);
        if (precision < 4 || precision > 16)
            throw new Exception(
                $"{nameof(WssEstimator)}: DeserializeInternalHeader, precision range error [{precision}]");

        var isFbs = ByteArrayHelper.ReadByte(serializedObject, index++);
        if (isFbs != 0 && isFbs != 1)
            throw new Exception(
                $"{nameof(WssEstimator)}: DeserializeInternalHeader, isFbs should be 0 or 1 [{isFbs}]");

        var fbs = ByteArrayHelper.ReadUInt32(serializedObject, index);
        index += 4;
        if (isFbs == 1)
            if (fbs == 0)
                throw new Exception(
                    $"{nameof(WssEstimator)}: DeserializeInternalHeader, isFbs==1 fbs cannot be 0");

        var minBlockSize = ByteArrayHelper.ReadUInt32(serializedObject, index);
        index += 4;

        var maxBlockSize = ByteArrayHelper.ReadUInt32(serializedObject, index);
        index += 4;

        var numInsersions = ByteArrayHelper.ReadInt64(serializedObject, index);
        index += 8;
        return (precision, isFbs == 1 ? true : false, fbs, minBlockSize, maxBlockSize, numInsersions);
    }

    private byte Precision;

    public void Merge(WssEstimator other)
    {
        for (int i = 0; i < _hlls.Length; i++)
        {
            _hlls[i].MergeCount(other._hlls[i]);
            NumInsertions += other.NumInsertions;
        }
    }

    public const string HllType = "HllOptimizedDense";
    public bool IsFixedBlockSize { get; set; }
    public uint FixedBlockSize { get; set; }
    public uint MaxBlockSize { get; set; }
    public uint MinBlockSize { get; set; }


    private double GetAvgDistinctBlockSize()
    {
        return (double)GetWss() / GetCardinality();
    }

    private int _numHlls;
    private HllOptimizedDense_Sparse[] _hlls;
    // private long[] _counts; //TODO:enable and marshal

    public WssEstimator()
    {
    }

    public WssEstimator(bool isFixedBlockSize, byte precision, uint fixedBlockSize, uint minBlockSize,
        uint maxBlockSize)
    {
        CtorInternal(isFixedBlockSize, precision, fixedBlockSize, minBlockSize, maxBlockSize, true);
    }

    private void CtorInternal(bool isFixedBlockSize, byte precision
        , uint fixedBlockSize, uint minBlockSize, uint maxBlockSize, bool initializeHlls)
    {
        Precision = precision;
        IsFixedBlockSize = isFixedBlockSize;
        FixedBlockSize = fixedBlockSize;
        MinBlockSize = minBlockSize;
        MaxBlockSize = maxBlockSize;

        if (isFixedBlockSize)
        {
            _hlls = new HllOptimizedDense_Sparse[1];
            if (initializeHlls)
            {
                _hlls[0] = new HllOptimizedDense_Sparse(precision, 0);
            }
            else
            {
                _hlls[0] = new();
            }

            _hlls[0].BlockSize = FixedBlockSize;
        }
        else
        {
            var maxNxtPow2 = NextPow2(MaxBlockSize);
            var minNxtPow2 = NextPow2(MinBlockSize);
            _numHlls = (int)(Log2(maxNxtPow2) - Log2(minNxtPow2) + 1);

            _hlls = new HllOptimizedDense_Sparse[_numHlls];
            for (int i = 0; i < _numHlls; i++)
            {
                if (initializeHlls)
                {
                    _hlls[i] = new HllOptimizedDense_Sparse(precision, i);
                }
                else
                {
                    _hlls[i] = new();
                }

                _hlls[i].BlockSize = minNxtPow2 << i;
            }
        }
    }

    private void AddReqsFbs(Request[] reqs, int startIdx, int endIdx)
    {
        NumberOfInserts += endIdx - startIdx;
        for (int i = startIdx; i < endIdx; i++)
        {
            _hlls[0].AddHash(reqs[i].KeyHash);
            AvgBlockSize = ((reqs[i].BlockSize - AvgBlockSize) / ++NumInsertedItems) + AvgBlockSize;
        }

        NumInsertions += endIdx - startIdx;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddFbs(ulong hash)
    {
        NumInsertions++;
        _hlls[0].AddHash(hash);
    }

    private void AddReqs(Request[] reqs, int startIdx, int endIdx)
    {
        if (IsFixedBlockSize)
        {
            AddReqsFbs(reqs, startIdx, endIdx);
        }
        else
        {
            NumberOfInserts += endIdx - startIdx;
            for (int i = startIdx; i < endIdx; i++)
            {
                var blockSize = NextPow2(reqs[i].BlockSize);
                if (blockSize > MaxBlockSize) blockSize = MaxBlockSize;
                var idx = Log2(blockSize) - 1;
                _hlls[idx].AddHash(reqs[i].KeyHash);
            }

            NumInsertions += endIdx - startIdx;
        }
    }

    public long NumInsertions;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Add(ulong hash, uint blockSize)
    {
        NumberOfInserts++;
        NumInsertions++;
        if (IsFixedBlockSize)
        {
            _hlls[0].AddHash(hash);
            AvgBlockSize = ((blockSize - AvgBlockSize) / ++NumInsertedItems) + AvgBlockSize;
        }
        else
        {
            var nxtPow2 = NextPow2(blockSize);
            var idx = Log2(nxtPow2) - 1;
            _hlls[idx].AddHash(hash);
        }
    }

    private long GetWss()
    {
        if (NumInsertions == 0) return 0;
        if (IsFixedBlockSize)
        {
            return (long)_hlls[0].Count() * FixedBlockSize;
        }
        else
        {
            long wss = 0;
            for (int i = 0; i < _hlls.Length; i++)
            {
                var count = _hlls[i].Count();
                // if (count > _counts[i]) count = _counts[i];
                wss += count * (1L << (i + 1));
            }

            return wss;
        }
    }

    private long GetCardinality()
    {
        if (NumInsertions == 0) return 0;
        if (IsFixedBlockSize)
        {
            return (long)_hlls[0].Count();
        }
        else
        {
            long cardinality = 0;
            for (int i = 0; i < _hlls.Length; i++)
            {
                var count = _hlls[i].Count();
                cardinality += count;
            }

            return cardinality;
        }
    }
}