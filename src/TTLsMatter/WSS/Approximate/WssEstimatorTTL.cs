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
public class WssEstimatorTtl : IWssCalculator
{
    private double AvgBlockSize = 0;
    private long NumInsertedItems = 0;
    public long NumberOfInserts { get; set; }

    #region Serialization tests

    public static void TestSerialization()
    {
        var rand = new Random(555);
        var minBlockSize = DatasetConfig.MinBlockSize;
        var maxBlockSize = DatasetConfig.MaxBlockSize;

        var numKeys = 1000 * 1000;
        var distinctMax = 100 * 1000;

        var estimatorVbs = new WssEstimatorTtl(false, 12, 4096, 2, 8 * 1024 * 1024);
        var estimatorFbs = new WssEstimatorTtl(true, 12, 4096, 0, 0);

        for (int i = 0; i < numKeys; i++)
        {
            var key = rand.Next(0, distinctMax);
            var hash = HashingHelper.MurmurHash264A((ulong)key);
            var bs = rand.Next((int)minBlockSize, (int)maxBlockSize);

            estimatorFbs.Add(hash, (uint)bs, 1000);
            estimatorVbs.Add(hash, (uint)bs, 1000);
        }

        var estimatorSizeFbs = estimatorFbs.GetWss(0);

        var serializedStaticFbs = estimatorFbs.SerializeStatic();
        var serializedDynamicFbs = estimatorFbs.SerializeDynamic();

        var deserializedStaticFbs = new WssEstimatorTtl();
        deserializedStaticFbs.Deserialize(serializedStaticFbs);
        var deserializedStaticFbsWss = deserializedStaticFbs.GetWss(0);

        var deserializedDynamicFbs = new WssEstimatorTtl();
        deserializedDynamicFbs.Deserialize(serializedDynamicFbs);
        var deserializedDynamicFbsWss = deserializedDynamicFbs.GetWss(0);

        Console.WriteLine(
            $"[FBS] REF WSS: {estimatorSizeFbs} DeserializedWssStatic: {deserializedStaticFbsWss} DeserializedWssDynamic: {deserializedDynamicFbsWss}");
        Console.WriteLine(
            $"[FBS] Size Bytes Static: {serializedStaticFbs.Length:n0} Dynamic: {serializedDynamicFbs.Length:n0}");

        //============================== vbs
        var estimatorSizeVbs = estimatorVbs.GetWss(0);

        var serializedStaticVbs = estimatorVbs.SerializeStatic();
        var serializedDynamicVbs = estimatorVbs.SerializeDynamic();

        var deserializedStaticVbs = new WssEstimatorTtl();
        deserializedStaticVbs.Deserialize(serializedStaticVbs);
        var deserializedStaticVbsWss = deserializedStaticVbs.GetWss(0);

        var deserializedDynamicVbs = new WssEstimatorTtl();
        deserializedDynamicVbs.Deserialize(serializedDynamicVbs);
        var deserializedDynamicVbsWss = deserializedDynamicVbs.GetWss(0);

        Console.WriteLine(
            $"[VBS] REF WSS: {estimatorSizeVbs} DeserializedWssStatic: {deserializedStaticVbsWss} DeserializedWssDynamic: {deserializedDynamicVbsWss}");
        Console.WriteLine(
            $"[VBS] Size Static: {serializedStaticVbs.Length:n0} Dynamic: {serializedDynamicVbs.Length:n0}");
    }

    #endregion

    #region TOOLS

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint NextPow2(uint val)
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
    public uint Log2(uint powOf2)
    {
        var toRetOptimized = 31 - Lzcnt.LeadingZeroCount(powOf2);
        return toRetOptimized;
    }

    #endregion

    #region INTERFACE MEMBERS

    public void AddRequest(Request request)
    {
        Add(request.KeyHash, request.BlockSize, request.EvictionTime);
    }

    public void AddRequests(Request[] requests, int startIdx, int endIdxExclusive)
    {
        AddReqs(requests, startIdx, endIdxExclusive);
    }

    public long GetWss_FixedBlockSize(uint currentTime)
    {
        if (IsFixedBlockSize) return GetWss(currentTime);
        return -1;
    }

    public long GetWss_VariableBlockSize(uint currentTime)
    {
        if (!IsFixedBlockSize) return GetWss(currentTime);
        return -1;
    }

    public long GetWss_VariableBlockSize_RunningAverage(uint currentTime)
    {
        if (IsFixedBlockSize) return (long)(GetCardinality(currentTime) * AvgBlockSize);
        return -1;
    }

    public long GetCardinality(uint currentTime)
    {
        return GetCardinality_internal(currentTime);
    }

    public void Clean()
    {
        foreach (var hll in _hlls)
        {
            if (hll != null)
                hll.Clean(1);
        }
    }

    private byte[] SerializeInternal(bool isStatic)
    {
        var header = SerializeInternalHeader();
        if (IsFixedBlockSize)
        {
            var serializedHll = (isStatic) ? _hlls[0].SerializeStatic() : _hlls[0].SerializeDynamic();
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
                var so = (isStatic) ? _hlls[i].SerializeStatic() : _hlls[i].SerializeDynamic();
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
            throw new Exception($"{nameof(WssEstimatorTtl)}:: Cannot deserialize from null object");
        if (serializedObject.Length <= 4)
            throw new Exception($"{nameof(WssEstimatorTtl)}:: Minimum serialized object size should be >4");

        int index = 0;
        var length = ByteArrayHelper.ReadUInt32(serializedObject, index);
        index += 4;
        if (length != serializedObject.Length)
            throw new Exception(
                $"{nameof(WssEstimatorTtl)}:: Serialized Object length should be {length} while actual length is {serializedObject.Length}");

        var header = DeserializeInternalHeader(serializedObject, ref index);
        var p = header.Item1;
        var isFbs = header.Item2;
        var fbs = header.Item3;
        var minbs = header.Item4;
        var maxbs = header.Item5;
        CtorInternal(isFbs, p, fbs, minbs, maxbs, false);


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
        var otherConv = (WssEstimatorTtl)other;
        for (int i = 0; i < NumHlls; i++)
        {
            _hlls[i].MergeCount(otherConv._hlls[i], 0, true);
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
         * =================================================
         * Header total bytes: 1+1+4+4+4=14 BYTES 
         */
        var header = new byte[14];
        ByteArrayHelper.WriteByte(header, 0, Precision);
        ByteArrayHelper.WriteByte(header, 1, IsFixedBlockSize ? (byte)1 : (byte)0);
        ByteArrayHelper.WriteUInt32(header, 2, FixedBlockSize);
        ByteArrayHelper.WriteUInt32(header, 6, MinBlockSize);
        ByteArrayHelper.WriteUInt32(header, 10, MaxBlockSize);
        return header;
    }

    private (byte, bool, uint, uint, uint) DeserializeInternalHeader(byte[] serializedObject, ref int index)
    {
        var precision = ByteArrayHelper.ReadByte(serializedObject, index++);
        if (precision < 4 || precision > 16)
            throw new Exception(
                $"{nameof(WssEstimatorTtl)}: DeserializeInternalHeader, precision range error [{precision}]");

        var isFbs = ByteArrayHelper.ReadByte(serializedObject, index++);
        if (isFbs != 0 && isFbs != 1)
            throw new Exception(
                $"{nameof(WssEstimatorTtl)}: DeserializeInternalHeader, isFbs should be 0 or 1 [{isFbs}]");

        var fbs = ByteArrayHelper.ReadUInt32(serializedObject, index);
        index += 4;
        if (isFbs == 1)
            if (fbs == 0)
                throw new Exception(
                    $"{nameof(WssEstimatorTtl)}: DeserializeInternalHeader, isFbs==1 fbs cannot be 0");

        var minBlockSize = ByteArrayHelper.ReadUInt32(serializedObject, index);
        index += 4;

        var maxBlockSize = ByteArrayHelper.ReadUInt32(serializedObject, index);
        index += 4;
        return (precision, isFbs == 1 ? true : false, fbs, minBlockSize, maxBlockSize);
    }

    public const string HllType = "HllOptimizedDense";
    public bool IsFixedBlockSize { get; set; }
    public uint FixedBlockSize { get; set; }
    public uint MaxBlockSize { get; set; }
    public uint MinBlockSize { get; set; }
    public byte Precision;

    public double GetAvgDistinctBlockSize(uint currentTime)
    {
        return (double)GetWss(currentTime) / GetCardinality(currentTime);
    }

    public int NumHlls;
    public HllBasicDenseTtl_Sparce[] _hlls;

    public WssEstimatorTtl()
    {
    }

    public WssEstimatorTtl(bool isFixedBlockSize, byte precision
        , uint fixedBlockSize, uint minBlockSize, uint maxBlockSize)
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
            _hlls = new HllBasicDenseTtl_Sparce[1];
            if (initializeHlls)
            {
                _hlls[0] = new HllBasicDenseTtl_Sparce(precision, 0);
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
            NumHlls = (int)(Log2(maxNxtPow2) - Log2(minNxtPow2) + 1);

            _hlls = new HllBasicDenseTtl_Sparce[NumHlls];
            for (int i = 0; i < NumHlls; i++)
            {
                if (initializeHlls)
                {
                    _hlls[i] = new HllBasicDenseTtl_Sparce(precision, i);
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
            _hlls[0].AddHash(reqs[i].KeyHash, reqs[i].EvictionTime);
            AvgBlockSize = ((reqs[i].BlockSize - AvgBlockSize) / ++NumInsertedItems) + AvgBlockSize;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddFbs(ulong hash, uint whenToEvict)
    {
        NumberOfInserts++;
        _hlls[0].AddHash(hash, whenToEvict);
    }

    public void AddReqs(Request[] reqs, int startIdx, int endIdx)
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
                    _hlls[Log2(blockSize) - 1].AddHash(reqs[i].KeyHash, reqs[i].EvictionTime);
                }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(ulong hash, uint blockSize, uint whenToEvict)
    {
        NumberOfInserts++;
        if (IsFixedBlockSize)
        {
            _hlls[0].AddHash(hash, whenToEvict);
            AvgBlockSize = ((blockSize - AvgBlockSize) / ++NumInsertedItems) + AvgBlockSize;
        }
        else
        {
            var nxtPow2 = NextPow2(blockSize);
            var idx = Log2(nxtPow2) - 1;
            _hlls[idx].AddHash(hash, whenToEvict);
        }
    }

    public long GetWss(uint currentTime)
    {
        if (IsFixedBlockSize)
        {
            return (long)_hlls[0].EvictExpiredBucketsAndCount(currentTime) * FixedBlockSize;
        }
        else
        {
            long wss = 0;
            for (int i = 0; i < _hlls.Length; i++)
            {
                wss += (long)_hlls[i].EvictExpiredBucketsAndCount(currentTime) * (1L << (i + 1));
            }

            return wss;
        }
    }

    private long GetCardinality_internal(uint currentTime)
    {
        if (IsFixedBlockSize)
        {
            return (long)_hlls[0].EvictExpiredBucketsAndCount(currentTime);
        }
        else
        {
            long cardinality = 0;
            for (int i = 0; i < _hlls.Length; i++)
                cardinality += (long)_hlls[i].EvictExpiredBucketsAndCount(currentTime);
            return cardinality;
        }
    }
}