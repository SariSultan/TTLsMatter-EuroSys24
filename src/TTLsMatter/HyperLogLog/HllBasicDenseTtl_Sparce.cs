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

using System.Numerics;
using System.Runtime.CompilerServices;
using TTLsMatter.Common.ByteArray;
using TTLsMatter.Common.Hashing;

namespace TTLsMatter.HyperLogLog;

/// <summary>
/// A HyperLogLog implementation that supports evictions based on TTL, HLL-TTL.
/// (c) Sari Sultan
/// </summary>
[Serializable]
public class HllBasicDenseTtl_Sparce
{
    #region SERIALIZATION

    public static void TestSerialization()
    {
        uint numberOfItems = 10 * 1000 * 1000;
        uint mod = 1000 * 1000;
        var minEvictionTime = 100;
        var maxEvictionTime = 1000;
        var rand = new Random(100);
        var hll = new HllBasicDenseTtl_Sparce(12);
        hll.BlockSize = 10;
        for (ulong i = 0; i < numberOfItems; i++)
        {
            var hash = HashingHelper.MurmurHash264A(i % mod);
            hll.AddHash(hash, (uint)rand.Next(minEvictionTime, maxEvictionTime));
        }

        var staticSerialized = hll.SerializeStatic();
        var dynamicSerialized = hll.SerializeDynamic();
        var countAt500 = hll.EvictExpiredBucketsAndCount(500);


        /*Try to restore and get the count, it should be the same*/
        var hllStatic = new HllBasicDenseTtl_Sparce();
        hllStatic.Deserialize(staticSerialized, 0, staticSerialized.Length);
        var countAt500Static = hllStatic.EvictExpiredBucketsAndCount(500);

        var hllDynamic = new HllBasicDenseTtl_Sparce();
        hllDynamic.Deserialize(dynamicSerialized, 0, dynamicSerialized.Length);
        var countAt500Dynamic = hllDynamic.EvictExpiredBucketsAndCount(500);

        hll.Prune();
        var count500AfterPrune = hll.EvictExpiredBucketsAndCount(500);
        if (!hll.IsSparse)
        {
            for (int i = 0; i < hll.NumberOfBuckets; i++)
            for (int j = 0; j < hll.MaxZeros; j++)
            {
                if (hll.Buckets[i, j] != hllDynamic.Buckets[i, j])
                {
                    Console.WriteLine($"Bucket Mismatch [{i}][{j}] ({hll.Buckets[i, j]}!={hllDynamic.Buckets[i, j]})");
                    throw new Exception("mismatch");
                }
            }
        }

        Console.WriteLine($"HLL IS.  Sparse: {hll.IsSparse}");
        Console.WriteLine(
            $"CountAt500. Ref: {countAt500} Static: {countAt500Static} Dynamic: {countAt500Dynamic}");
        Console.WriteLine(
            $"Size.  Static: {staticSerialized.Length / 1024.0:f4}KB Dynamic: {dynamicSerialized.Length / 1024.0:f4}KB");
        Console.WriteLine($"Size. Ref: {countAt500} Static: {countAt500Static} Dynamic: {countAt500Dynamic}");
    }

    public HllBasicDenseTtl_Sparce()
    {
    }

    /// <summary>
    /// This means Sparse OR dynamic buckets as needed
    /// </summary>
    /// <returns></returns>
    public byte[] SerializeDynamic()
    {
        return IsSparse ? SerializeInternalSparse() : SerializeInternalDenseDynamic();
    }

    /// <summary>
    /// This means we always return the full buckets 
    /// </summary>
    /// <returns></returns>
    public byte[] SerializeStatic()
    {
        return IsSparse ? SerializeInternalSparse() : SerializeInternalDenseStatic();
    }

    public void Deserialize(byte[] serializedObject, int startIdx, int endIdxExclusive)
    {
        Used = true;
        if (serializedObject == null) throw new Exception("Cannot deserialize from null object");
        if (serializedObject.Length <= 4)
            throw new Exception(
                $"Cannot deserialize because the serialized object has very small length (i.e., {serializedObject.Length})");

        /*Determine the type, and deserialize based on that*/
        var actualSize = endIdxExclusive - startIdx;
        var index = startIdx;
        var arraySize = ByteArrayHelper.ReadUInt32(serializedObject, index);
        index += 4;
        if (arraySize != actualSize)
            throw new Exception(
                $"Cannot deserialize object because it says the length should be {arraySize} bytes, while the actual length is {actualSize}");

        /*Deserialize Header*/
        var headerVals = DeserializeHeader(serializedObject, ref index);
        var blockSize = headerVals.Item1;
        var precision = headerVals.Item2;
        var isSparse = headerVals.Item3;
        var isStatic = headerVals.Item4;

        BlockSize = blockSize;
        CtorInternal(precision);

        IsSparse = isSparse;

        if (isSparse)
        {
            /*Deserialize Sparse*/
            var numberOfObjects = ByteArrayHelper.ReadUInt32(serializedObject, index);
            index += 4;

            var expectedEndIdx = index + numberOfObjects * 12;
            if (endIdxExclusive != expectedEndIdx)
                throw new Exception(
                    $"Error in deserialize: (Sparse). Expected end index: {expectedEndIdx} while actual end index: {endIdxExclusive}");

            for (int i = 0; i < numberOfObjects; i++)
            {
                var key = ByteArrayHelper.ReadUInt64(serializedObject, index);
                index += 8;
                var et = ByteArrayHelper.ReadUInt32(serializedObject, index);
                index += 4;
                SparceHashSet.Add(key, et);
            }

            LastCount = SparceHashSet.Count;
        }
        else if (isStatic)
        {
            /*Deserialize static*/
            isSparse = false;
            InitBuckets();
            SparceHashSet = null;

            var expectedEndIdx = index + (uint)(NumberOfBuckets * MaxZeros * sizeof(uint));
            if (endIdxExclusive != expectedEndIdx)
                throw new Exception(
                    $"Error in deserialize: (Dense-Static). Expected end index: {expectedEndIdx} while actual end index: {endIdxExclusive}");
            for (int bucketIdx = 0; bucketIdx < NumberOfBuckets; bucketIdx++)
            for (byte nzeros = 0; nzeros < MaxZeros; nzeros++)
            {
                var et = ByteArrayHelper.ReadUInt32(serializedObject, index);
                Buckets[bucketIdx, nzeros] = et;
                index += 4;
                if (et != 0) BucketPositions[bucketIdx] = nzeros;
            }
        }
        else
        {
            /*Deserialize dynamic*/
            isSparse = false;
            InitBuckets();
            SparceHashSet = null;

            /*TODO:: Verify length*/
            /*ROWINDX(uint32)|NumNLZs(uint32) --> each NLZ takes 5 bytes: NLZ (byte) | ET (uint)*/
            while (index + 8 + 5 <= endIdxExclusive)
            {
                var bucketIdx = ByteArrayHelper.ReadUInt32(serializedObject, index);
                index += 4;
                var numNlzs = ByteArrayHelper.ReadUInt32(serializedObject, index);
                index += 4;
                for (int i = 0; i < numNlzs; i++)
                {
                    var nlz = ByteArrayHelper.ReadByte(serializedObject, index++);


                    var et = ByteArrayHelper.ReadUInt32(serializedObject, index);
                    index += 4;
                    Buckets[bucketIdx, nlz] = et;
                    if (et != 0)
                        if (BucketPositions[bucketIdx] <
                            nlz) //im so stupid, i serialized them in reverse order lol, this solves the issue 
                            BucketPositions[bucketIdx] = nlz; /*we take furthest to the right that is not zero*/
                }
            }

            if (index != endIdxExclusive)
                throw new Exception(
                    $"Error in deserialize: (Dense-Dynamic). Expected end index: {index} while actual end index: {endIdxExclusive}");
        }
    }

    private byte[] SerializeInternalSparse()
    {
        if (!IsSparse) throw new Exception("Cannot serialize sparse while the sparse flag is not set");

        var header = SerializeInternalHeader(true);
        /*
         * FIELD 1: THE NUMBER OF OBJECTS (UINT32)
         * FIELD 2: We need to output <Hash(uint64),EvictionTime(uint32)> pairs (12 BYTES * N)
         */
        uint numberOfObjects = (uint)SparceHashSet.Count;
        uint arraySize = 4 //length
                         + (uint)header.Length
                         + 4 //for the number of objects 
                         + numberOfObjects * 12;
        var serialized = new byte[arraySize];

        var index = 0;
        ByteArrayHelper.WriteUInt32(serialized, index, arraySize);
        index += 4;
        for (int i = index; i < header.Length + index; i++)
            serialized[i] = header[i - index];
        index += header.Length;

        ByteArrayHelper.WriteUInt32(serialized, index, numberOfObjects);
        index += 4;

        foreach (var e in SparceHashSet)
        {
            ByteArrayHelper.WriteUInt64(serialized, index, e.Key);
            index += 8;
            ByteArrayHelper.WriteUInt32(serialized, index, e.Value);
            index += 4;
        }

        return serialized;
    }


    private byte[] SerializeInternalDenseDynamic()
    {
        var header = SerializeInternalHeader(false);

        /*if it was dense then it will be more interesting*/
        /*We need first to know the requirements, which can be found by checking invariants 1+2 (to insure the minimum space requirement)*/
        uint numberOfEvictionTimesToBeRecorded =
            0; /*Each record needs NLZ (BYTE) + EvictionTime (uint32) = 5 bytes*/
        uint numberOfRows = 0; /*Each row need RowIdx (uint32) + NumberOfNlzs(uint32) = 8 bytes*/
        var numNlzsPerRow = new Dictionary<int, int>();
        for (int bucketIdx = 0; bucketIdx < NumberOfBuckets; bucketIdx++)
        {
            uint prev_et = 0;
            var addRow = false;
            var nNlzsForThisRow = 0;
            for (int nlz = MaxZeros - 1; nlz >= 0; nlz--)
            {
                /*INVARIANT 2*/
                if (Buckets[bucketIdx, nlz] > prev_et)
                {
                    if (!addRow)
                    {
                        addRow = true;
                        numberOfRows++;
                    }

                    prev_et = Buckets[bucketIdx, nlz];
                    numberOfEvictionTimesToBeRecorded++;
                    nNlzsForThisRow++;
                }
            }

            if (addRow)
                numNlzsPerRow.Add(bucketIdx, nNlzsForThisRow);
        }

        uint arraySize = 4 //for the total length (this allows me to stop processing)
                         + (uint)header.Length
                         + 8 * numberOfRows //for the rows where each row has its index (uint) and number of nlzs (uint)
                         + 5 *
                         numberOfEvictionTimesToBeRecorded; //for nlzs where each one has nlz (byte) and evictiontime(uint)

        var serialized = new byte[arraySize];
        var index = 0;
        ByteArrayHelper.WriteUInt32(serialized, index, arraySize);
        index += 4;
        for (int i = index; i < header.Length + index; i++)
            serialized[i] = header[i - index];
        index += header.Length;

        for (int bucketIdx = 0; bucketIdx < NumberOfBuckets; bucketIdx++)
        {
            if (!numNlzsPerRow.ContainsKey(bucketIdx)) continue;

            var nNlzs = numNlzsPerRow[bucketIdx];
            ByteArrayHelper.WriteInt32(serialized, index, bucketIdx);
            index += 4;
            ByteArrayHelper.WriteInt32(serialized, index, nNlzs);
            index += 4;


            uint prev_et = 0;
            for (int nlz = MaxZeros - 1; nlz >= 0; nlz--)
            {
                /*INVARIANT 2*/
                if (Buckets[bucketIdx, nlz] > prev_et)
                {
                    ByteArrayHelper.WriteByte(serialized, index, (byte)nlz);
                    index += 1;

                    ByteArrayHelper.WriteUInt32(serialized, index, Buckets[bucketIdx, nlz]);
                    index += 4;

                    prev_et = Buckets[bucketIdx, nlz];
                }
            }
        }

        return serialized;
    }

    private byte[] SerializeInternalDenseStatic()
    {
        var header = SerializeInternalHeader(true);

        uint arraySize = 4 // for the length
                         + (uint)header.Length
                         + (uint)(NumberOfBuckets * MaxZeros * sizeof(uint));

        var serialized = new byte[arraySize];

        int index = 0;
        ByteArrayHelper.WriteUInt32(serialized, index, arraySize);
        index += 4;
        for (int i = index; i < header.Length + index; i++)
            serialized[i] = header[i - index];
        index += header.Length;
        for (int bucketIdx = 0; bucketIdx < NumberOfBuckets; bucketIdx++)
        for (int nlz = 0; nlz < MaxZeros; nlz++)
        {
            ByteArrayHelper.WriteUInt32(serialized, index, Buckets[bucketIdx, nlz]);
            index += 4;
        }

        return serialized;
    }

    /// <summary>
    /// Deserializes the header and increments the index
    /// </summary>
    /// <param name="serializedObject"></param>
    /// <param name="index"></param>
    /// <returns></returns>
    private (uint, byte, bool, bool) DeserializeHeader(byte[] serializedObject, ref int index)
    {
        var blockSize = ByteArrayHelper.ReadUInt32(serializedObject, index);
        index += 4;
        var precision = ByteArrayHelper.ReadByte(serializedObject, index++);
        if (precision < 4 || precision > 16)
            throw new Exception(
                $"Unable to deserialize header because precision value should be [4-16] while its {precision}");
        var isSparceByte = ByteArrayHelper.ReadByte(serializedObject, index++);
        if (isSparceByte != 0 && isSparceByte != 1)
            throw new Exception(
                $"Unable to deserialize header because IsSparceByte value should be [0,1] while its {isSparceByte}");

        var isSparce = isSparceByte == 1 ? true : false;

        var isStaticByte = ByteArrayHelper.ReadByte(serializedObject, index++);
        if (isStaticByte != 0 && isStaticByte != 1)
            throw new Exception(
                $"Unable to deserialize header because isStaticByte value should be [0,1] while its {isStaticByte}");

        var isStatic = isStaticByte == 1 ? true : false;
        return (blockSize, precision, isSparce, isStatic);
    }

    private byte[] SerializeInternalHeader(bool isStatic)
    {
        /*
         * INDEX : FIELD : NUMBER OF BYTES : EXTRA COMMENTS
         * ================================================
         * 0    : block size  : uint 4 bytes
         * 1     : Precision   : 1 BYTE : [4-16]
         * 2    : IsSparse     : 1 BYTE : if it was sparse, we always output the hashtable
         * 3    : IsStatic    :  1 BYTE : don't care is is sparse.
         * ===============================================
         * Header length: 7 bytes
         */
        var header = new byte[7];
        if (BlockSize == 0) throw new Exception("Block size should be set for serialization");
        ByteArrayHelper.WriteUInt32(header, 0, BlockSize);
        ByteArrayHelper.WriteByte(header, 4, Precision);
        ByteArrayHelper.WriteByte(header, 5, IsSparse ? (byte)1 : (byte)0);
        ByteArrayHelper.WriteByte(header, 6, isStatic ? (byte)1 : (byte)0);
        return header;
    }


    private void Prune()
    {
        if (IsSparse) return;
        for (int bucketIdx = 0; bucketIdx < NumberOfBuckets; bucketIdx++)
        {
            uint prev_et = 0;
            for (int nlz = MaxZeros - 1; nlz >= 0; nlz--)
            {
                /*INVARIANT 2*/
                if (Buckets[bucketIdx, nlz] > prev_et)
                {
                    prev_et = Buckets[bucketIdx, nlz];
                }
                else
                {
                    Buckets[bucketIdx, nlz] = 0;
                }
            }
        }
    }

    #endregion SERIALIZATION

    private bool IsSparse;
    private byte MaxZeros = 32; /*should be set to at most 64-b*/
    private bool Used = false;
    public int SparseCapacity; /*not needed in serialization because it is computed*/
    public Dictionary<ulong, uint> SparceHashSet;

    #region Public properties

    /// <summary>
    /// HLL buckets.
    /// The number of buckets are 2^ <see cref="Precision"/>
    /// Precision is between [4-16].
    ///
    /// Eviction times two-dimentional array.
    /// The first index is accessed using the bucket index.
    /// The second index is accessed using the number of zeros in that hash.
    ///
    /// the second dimension length is (64-b+1) since we count number of zeros (+1) in the hash.
    ///
    /// <see cref="https://stackoverflow.com/questions/597720/what-are-the-differences-between-a-multidimensional-array-and-an-array-of-arrays"/>
    /// It seems that jagged arrays are faster.
    /// After testing both, i found jagged to be around 3% faster. 
    /// </summary>
    public uint[,] Buckets { get; private set; }
    // public uint[][] Buckets { get; private set; }

    /// <summary>
    /// This is a helper array to have faster access for the count operation.
    /// If this array is not used then for each count operation we need to find the largest bucket value that is set.
    /// With this array, we directly access it.
    /// </summary>
    public byte[] BucketPositions { get; private set; }

    /// <summary>
    /// 64-b+1
    /// </summary>
    public int LargestNumberOfZeros { get; private set; }

    //public int[] BucketListCounts { get; private set; }
    /// <summary>
    /// HLL precision parameter.
    /// </summary>
    public byte Precision;

    /// <summary>
    /// Number of buckets = 2^ <see cref="Precision"/>
    /// </summary>
    public int NumberOfBuckets;

    /// <summary>
    /// Optional Id to identify the Hyperloglog object.
    /// </summary>
    public int Id { get; private set; }

    /// <summary>
    /// Optional variable to store how many instances are using this hyperlolog.
    /// </summary>
    public int ReferenceCount;

    /// <summary>
    /// Optional variable to store a block size corresponding to this hll
    /// </summary>
    public uint BlockSize;

    /// <summary>
    /// The last count reported by this HyperLogLog
    /// </summary>
    public long LastCount;

    #endregion Public properties

    #region Private Variables

    /// <summary>
    /// The shift required to get the bucket index.
    /// We need <see cref="Precision"/> number of bits to access the bucket
    /// from <see cref="Buckets"/>
    /// Unlike other algorithms such as Microsofts and Redis, we
    /// take the first <see cref="Precision"/> bits from MSB.
    /// Assuming precision is 16:
    /// MSB .................... LSB
    /// [63,62,...,49,48,47,...,1,0]
    ///  ^--Bktindex--^  ^-nZeros-^
    /// </summary>
    public byte BucketIndexShift;

    /// <summary>
    /// In order to determine the number of zeros in the hash we
    /// need to make sure that the LSB bit of the bits used for index is set to 1.
    /// Otherwise, we might go beyond it and compute number of zeros larger than possible.
    /// Hence, this flag is used as an bitwise OR mask to set that bit.
    /// For example, assume we use a 16 precision.
    /// MSB .................... LSB
    /// [63,62,...,49,48,47,...,1,0]
    ///  ^--Bktindex--^  ^-nZeros-^
    ///      ----->   ^ (bit 48 will be set before counting zeros)
    /// </summary>
    private ulong _bucketIndexLsbSetMask;

    /// <summary>
    /// Bias correction constant. 
    /// See Equation (3) [Hyperloglog: the analysis of a near-optimal cardinality estimation algorithm]
    /// and the corresponding values for it from [HyperLogLog in practice: algorithmic engineering of a state of the art cardinality estimation algorithm]
    /// </summary>
    private double _alphaM;

    /// <summary>
    /// This is the correction bias constant <see cref="_alphaM"/>
    /// multiplied by number of buckets <see cref="NumberOfBuckets"/> squared.
    /// It is stored in this variable to avoid computing it over and over each
    /// time we are doing a computation. 
    /// </summary>
    private double _alphaMmSquared;

    /// <summary>
    /// The value of this variable is 5*<see cref="NumberOfBuckets"/>.
    /// It is stored here to avoid doing the operation over an over.
    /// </summary>
    private double _fiveM;

    /// <summary>
    /// the threshold determining whether to use LinearCounting or HyperLogLog for an estimate.
    /// Values are from [HyperLogLog in practice: algorithmic engineering of a state of the art cardinality estimation algorithm]
    /// See <see cref="HllOptimizationHelpers.GetSubAlgorithmSelectionThreshold"/> for its value based on <see cref="Precision"/>.
    /// </summary>
    private double _subAlgorithmSelectionThreshold;

    #endregion Private Variables

    #region Constructor

    private void InitBuckets()
    {
        if (IsSparse) return;
        Buckets = new uint[NumberOfBuckets, LargestNumberOfZeros];
        BucketPositions = new byte[NumberOfBuckets];
    }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="precision">Hyperloglog precision in [4,16]</param>
    /// <param name="id">optional id</param>
    public HllBasicDenseTtl_Sparce(byte precision, int id = 0, byte maxNumzeros = 32)
    {
        MaxZeros = maxNumzeros;
        CtorInternal(precision, id);
    }

    private void CtorInternal(byte precision, int id = 0)
    {
        if (precision < 4 || precision > 16)
        {
            throw new Exception("Hyperloglog initialization error." +
                                "Precision should be in [4,16], " +
                                $"the value {precision} was provided instead.");
        }

        Precision = precision;
        Id = id;
        ReferenceCount = 0;
        NumberOfBuckets = 1 << Precision;
        LargestNumberOfZeros = (64 - Precision + 1) + 1;

        if (LargestNumberOfZeros > MaxZeros) LargestNumberOfZeros = MaxZeros;

        var sizeBytes = MaxZeros * 4 * (1 << precision);
        SparseCapacity = sizeBytes / (sizeof(ulong) + sizeof(uint)) +1;

        if (IsSparse)
            SparceHashSet = new Dictionary<ulong, uint>(SparseCapacity);
        else
            InitBuckets();

        BucketIndexShift = (byte)(64 - Precision);
        _bucketIndexLsbSetMask = 1UL << BucketIndexShift;

        if (NumberOfBuckets == 16) _alphaM = 0.673;
        else if (NumberOfBuckets == 32) _alphaM = 0.697;
        else if (NumberOfBuckets == 64) _alphaM = 0.709;
        else _alphaM = 0.7213 / (1 + 1.079 / NumberOfBuckets);

        _alphaMmSquared = _alphaM * NumberOfBuckets * NumberOfBuckets;
        _fiveM = 5 * NumberOfBuckets;
        _subAlgorithmSelectionThreshold = HllOptimizationHelpers.GetSubAlgorithmSelectionThreshold(Precision);
    }

    #endregion

    #region Public methods

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddHash(ulong hash, uint evictionTime)
    {
        if (!Used) Used = true;

        if (IsSparse)
        {
            if (SparceHashSet.Count + 1 >= SparseCapacity)
            {
                IsSparse = false;
                InitBuckets();
                foreach (var e in SparceHashSet)
                    AddHash(e.Key, e.Value);

                SparceHashSet = null;
            }
            else
            {
                SparceHashSet.TryAdd(hash, evictionTime);
                return;
            }
        }


        var bucketIdx = (int)(hash >> BucketIndexShift);
        byte numberOfZeros = (byte)(BitOperations.TrailingZeroCount(hash) + 1);
        if (numberOfZeros > BucketIndexShift)
            numberOfZeros = BucketIndexShift; //used to remove | BucketIndexLSBsetMask

        if (numberOfZeros >= MaxZeros)
            numberOfZeros = (byte)(MaxZeros - 1);
        /* Since the number of zeros are guaranteed to be equal to the actual position, we need to replace the eviction time if it was larger.*/
        /* If the previous eviction time is zero, this means that the subbucket is being accessed for the first time, hence
         * the positions array should be updated if the last position for this bucket is less than the current number of zeros.
         */

        if (BucketPositions[bucketIdx] < numberOfZeros)
            BucketPositions[bucketIdx] = numberOfZeros;

        // if (counter.Buckets[bucketIdx][numberOfZeros] < evictionTime)
        //     counter.Buckets[bucketIdx][numberOfZeros] = evictionTime;
        if (Buckets[bucketIdx, numberOfZeros] < evictionTime)
            Buckets[bucketIdx, numberOfZeros] = evictionTime;
    }

    private long last_merge_sn = 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long MergeCount(HllBasicDenseTtl_Sparce newHll, long merge_sn = 0, bool over = false)
    {
        if (!Used) Used = true;
        if (merge_sn <= last_merge_sn) 
            return LastCount;
        last_merge_sn = merge_sn;
        
        if (Id == newHll.Id && !over) return LastCount = Count();


        if (newHll.IsSparse)
        {
            if (IsSparse)
            {
                foreach (var entry in newHll.SparceHashSet)
                {
                    if (IsSparse)
                    {
                        /*Its important to do it this way because it might convert from sparse to dense*/
                        if (!SparceHashSet.ContainsKey(entry.Key))
                                AddHash(entry.Key, entry.Value);
                    }
                    else
                    {
                        //here the type changed
                        AddHash(entry.Key, entry.Value);
                    }
                }

                if (IsSparse) return LastCount = SparceHashSet.Count;
                else return LastCount = Count();
            } //! if (IsSparce)
            else
            {
                foreach (var entry in newHll.SparceHashSet)
                    AddHash(entry.Key, entry.Value);

                return LastCount = Count();
            }
        }
        else //! if (otherHll.IsSparce)
        {
            if (IsSparse)
            {
                IsSparse = false;
                InitBuckets();
                foreach (var entry in SparceHashSet)
                {
                    AddHash(entry.Key, entry.Value);
                }

                SparceHashSet = null;
            }
            else
            {
                /*if both were dense*/
                /*no need to handle anything here because it is handled below*/
            }
        }


        /*The new hll was just created, hence we need to iterate on all of its buckets in the bucket list*/
        double zInverse = 0;
        var vInt = 0;
        //The merge will add a new entry into the buckets
        //the order bucket method should be called after this (for now i'm doing it explicitly)
        for (var i = 0; i < NumberOfBuckets; i++)
        {
            /*Iterate on all of the new hll sub buckets*/
            for (int j = newHll.BucketPositions[i]; j > 0; j--)
                if (Buckets[i, j] < newHll.Buckets[i, j])
                {
                    Buckets[i, j] = newHll.Buckets[i, j];
                    if (j > BucketPositions[i])
                        BucketPositions[i] = (byte)j;
                }

            /*Finally add the count based on the latest sub bucket*/
            if (BucketPositions[i] == 0) vInt++;
            else zInverse += 1.0 / (1 << BucketPositions[i]);
        }

        double v = vInt;
        zInverse += v;

        double e = _alphaMmSquared / zInverse;
        if (e <= _fiveM)
        {
            e = BiasCorrection.CorrectBias(e, Precision);
        }

        double h;
        if (v > 0)
        {
            // LinearCounting estimate
            h = NumberOfBuckets * Math.Log(NumberOfBuckets * 1.0 / v);
        }
        else
        {
            h = e;
        }

        if (h <= _subAlgorithmSelectionThreshold)
        {
            LastCount = (long)Math.Round(h);
            return LastCount;
        }

        LastCount = (long)Math.Round(e);
        return LastCount;
    }


    /*
     * DO NOT DEPEND ON THE REPORTED COUNT WHILE IN A PARALLEL LOOP
     */
    public long EvictExpiredBucketsAndCount(uint currentTime)
    {
        if (!Used)
        {
            return 0;
        }

        if (IsSparse)
        {
            var keysToRemoveIdx = 0;
            var keysToRemove = new List<ulong>(10);
            foreach (var entry in SparceHashSet)
                if (entry.Value <= currentTime)
                    keysToRemove.Add( entry.Key);

            foreach (var kUlong in keysToRemove)
            {
                SparceHashSet.Remove(kUlong);
            }
            
            return LastCount = SparceHashSet.Count;
        }


        double zInverse = 0;
        var vInt = 0;

        /*We iterate over all the buckets*/
        for (int i = 0; i < NumberOfBuckets; i++)
        {
            for (int j = BucketPositions[i]; j > 0; j--)
            {
                if (Buckets[i, j] != 0)
                    if (Buckets[i, j] <= currentTime) //MUST BE <= (makes big difference)
                    {
                        Buckets[i, j] = 0;

                        /*if this was the largest subbucket then reset bucket positions*/
                        if (j == BucketPositions[i])
                            BucketPositions[i] = 0;
                    }
                    else if (j > BucketPositions[i]) /*given that the bucket is not zero, then this is the largest*/
                    {
                        BucketPositions[i] = (byte)j;
                    }
            }

            /*Count part*/
            if (BucketPositions[i] == 0) vInt++;
            else zInverse += 1.0 / (1 << BucketPositions[i]);
        }


        double v = vInt;
        zInverse += v;

        double e = _alphaMmSquared / zInverse;
        if (e <= _fiveM)
        {
            e = BiasCorrection.CorrectBias(e, Precision);
        }

        double h;
        if (v > 0)
        {
            // LinearCounting estimate
            h = NumberOfBuckets * Math.Log(NumberOfBuckets * 1.0 / v);
        }
        else
        {
            h = e;
        }

        if (h <= _subAlgorithmSelectionThreshold)
        {
            LastCount = (long)Math.Round(h);
            return LastCount;
        }

        LastCount = (long)Math.Round(e);
        return LastCount;
    }

    /// <summary>
    /// Perform HLL count operation
    /// </summary>
    /// <returns>count</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Count()
    {
        if (!Used) return 0;
        if (IsSparse) return LastCount = SparceHashSet.Count;

        double zInverse = 0;
        int v = 0;
        for (var i = 0; i < NumberOfBuckets; i++)
            if (BucketPositions[i] == 0) v++;
            else zInverse += 1.0 / (1 << BucketPositions[i]);

        zInverse += v;

        double e = _alphaMmSquared / zInverse;
        if (e <= _fiveM)
        {
            e = BiasCorrection.CorrectBias(e, Precision);
        }

        double h;
        if (v > 0)
        {
            // LinearCounting estimate
            h = NumberOfBuckets * Math.Log(NumberOfBuckets * 1.0 / v);
        }
        else
        {
            h = e;
        }

        if (h <= _subAlgorithmSelectionThreshold)
        {
            LastCount = (long)Math.Round(h);
            return LastCount;
        }

        LastCount = (long)Math.Round(e);
        return LastCount;
    }

    #endregion

    public void Clean(int dop)
    {
        if (!Used) return;

        if (IsSparse)
        {
            SparceHashSet = new();
        }
        else
        {
            Array.Clear(Buckets, 0, Buckets.Length);
            Array.Clear(BucketPositions, 0, NumberOfBuckets);
        }

        /*reset the last count*/
        LastCount = 0;
        BlockSize = 0;
    }
}