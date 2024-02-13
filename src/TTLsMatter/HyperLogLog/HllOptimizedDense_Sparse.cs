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

[Serializable]
public class HllOptimizedDense_Sparse
{
    #region Serialization

     public static void TestSerialization()
    {
        uint numberOfItems = 10*1000 * 1000;
        uint mod = 1000 * 1000;
        var hll = new HllOptimizedDense_Sparse(12);
        hll.BlockSize = 10;
        for (ulong i = 0; i < numberOfItems; i++)
        {
            var hash = HashingHelper.MurmurHash264A(i % mod);
            hll.AddHash(hash);
        }

        var count = hll.Count();
        var staticSerialized = hll.Serialize();

        /*Try to restore and get the count, it should be the same*/
        var hllStatic = new HllOptimizedDense_Sparse();
        hllStatic.Deserialize(staticSerialized, 0, staticSerialized.Length);
        var countSerialized = hllStatic.Count();

        Console.WriteLine($"HLL IS.  Sparse: {hll.IsSparse}");
        Console.WriteLine($"CountAt500. Ref: {count} Static: {countSerialized}");
    }

     public HllOptimizedDense_Sparse()
     {
         
     }
    /// <summary>
    /// This means Sparse OR dynamic buckets as needed
    /// </summary>
    /// <returns></returns>
    public byte[] Serialize()
    {
        return IsSparse ? SerializeInternalSparse() : SerializeInternalDense();
    }
      public void Deserialize(byte[] serializedObject, int startIdx, int endIdxExclusive)
    {
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
        var numInsersions = headerVals.Item5;

        BlockSize = blockSize;
        TotalNumberOfInsertedElements = numInsersions;

        CtorInternal(precision);

        IsSparse = isSparse;

        if (isSparse)
        {
            /*Deserialize Sparse*/
            var numberOfObjects = ByteArrayHelper.ReadUInt32(serializedObject, index);
            index += 4;

            var expectedEndIdx = index + numberOfObjects * 8;
            if (endIdxExclusive != expectedEndIdx)
                throw new Exception(
                    $"Error in deserialize: (Sparse). Expected end index: {expectedEndIdx} while actual end index: {endIdxExclusive}");

            for (int i = 0; i < numberOfObjects; i++)
            {
                var key = ByteArrayHelper.ReadUInt64(serializedObject, index);
                index += 8;
                SparceHashSet.Add(key);
            }

            LastCount = SparceHashSet.Count;
        }
        else
        {
            /*Deserialize static*/
            Buckets = new byte[NumberOfBuckets];
            SparceHashSet = null;

            var expectedEndIdx = index + (uint)(NumberOfBuckets );
            if (endIdxExclusive != expectedEndIdx)
                throw new Exception(
                    $"Error in deserialize: (Dense-Static). Expected end index: {expectedEndIdx} while actual end index: {endIdxExclusive}");
            for (int bucketIdx = 0; bucketIdx < NumberOfBuckets; bucketIdx++)
            {
                var nlz = ByteArrayHelper.ReadByte(serializedObject, index++);
                Buckets[bucketIdx] = nlz;
            }
        }
    }
      

    private byte[] SerializeInternalSparse()
    {
        if (!IsSparse) throw new Exception("Cannot serialize sparse while the sparse flag is not set");

        var header = SerializeInternalHeader(true);
        /*
            * FIELD 1: THE NUMBER OF OBJECTS (UINT32)
            * FIELD 2: We need to output Hash(uint64)  (8 BYTES * N)
            */
        uint numberOfObjects = (uint)SparceHashSet.Count;
        uint arraySize = 4 //length
                         + (uint)header.Length
                         + 4 //for the number of objects 
                         + numberOfObjects * 8;
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
            ByteArrayHelper.WriteUInt64(serialized, index, e);
            index += 8;
        }

        return serialized;
    }
    
    private byte[] SerializeInternalDense()
    {
        var header = SerializeInternalHeader(true);

        uint arraySize = 4 // for the length
                         + (uint)header.Length
                         + (uint)(NumberOfBuckets);

        var serialized = new byte[arraySize];

        int index = 0;
        ByteArrayHelper.WriteUInt32(serialized, index, arraySize);
        index += 4;
        for (int i = index; i < header.Length + index; i++)
            serialized[i] = header[i - index];
        index += header.Length;
        for (int bucketIdx = 0; bucketIdx < NumberOfBuckets; bucketIdx++)
            ByteArrayHelper.WriteByte(serialized, index++, Buckets[bucketIdx]);
        return serialized;
    }
    

    /// <summary>
    /// Deserializes the header and increments the index
    /// </summary>
    /// <param name="serializedObject"></param>
    /// <param name="index"></param>
    /// <returns></returns>
    private (uint, byte, bool, bool, long) DeserializeHeader(byte[] serializedObject, ref int index)
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

        var numInsersions = ByteArrayHelper.ReadInt64(serializedObject, index);
        index += 8;
        return (blockSize, precision, isSparce, isStatic, numInsersions);
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
         * 4    : NumInsertions : long 8bytes 
        * ===============================================
        * Header length: 15 bytes
        */
        var header = new byte[15];
        if (BlockSize == 0) throw new Exception("Block size should be set for serialization");
        ByteArrayHelper.WriteUInt32(header, 0, BlockSize);
        ByteArrayHelper.WriteByte(header, 4, Precision);
        ByteArrayHelper.WriteByte(header, 5, IsSparse ? (byte)1 : (byte)0);
        ByteArrayHelper.WriteByte(header, 6, isStatic ? (byte)1 : (byte)0);
        ByteArrayHelper.WriteInt64(header, 7, TotalNumberOfInsertedElements);
        return header;
    }

    #endregion Serialization

    public long TotalNumberOfInsertedElements = 0;
    public bool IsSparse = false;
    public int SparseCapacity;
    public HashSet<ulong> SparceHashSet;

    #region Public properties

    /// <summary>
    /// HLL buckets.
    /// The number of buckets are 2^ <see cref="Precision"/>
    /// Precision is between [4-16].
    /// </summary>
    public byte[] Buckets { get; private set; }

    /// <summary>
    /// HLL precision parameter.
    /// </summary>
    public byte Precision;

    /// <summary>
    /// Number of buckets = 2^ <see cref="Precision"/>
    /// </summary>
    public int NumberOfBuckets;

    /*TODO: the following three parameters should be taken out from this implementation
         they are not needed to be here. 
        */

    /// <summary>
    /// Optional Id to identify the Hyperloglog object.
    /// </summary>
    public int Id { get; private set; }

    /// <summary>
    /// Optional variable to store how many instances are using this hyperlolog.
    /// </summary>
    public int NumberOfUsers;

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
    private byte _bucketIndexShift;

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

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="precision">Hyperloglog precision in [4,16]</param>
    /// <param name="id">optional id</param>
    public HllOptimizedDense_Sparse(byte precision, int id = 0)
    {
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

        Id = id;
        NumberOfUsers = 0;

        Precision = precision;
        NumberOfBuckets = 1 << precision;
        _bucketIndexShift = (byte)(64 - precision);
        _bucketIndexLsbSetMask = 1UL << _bucketIndexShift;
        
        var sizeBytes = 1 << precision;
        if (IsSparse)
        {
            SparseCapacity = sizeBytes / sizeof(ulong);
            SparceHashSet = new HashSet<ulong>(SparseCapacity);
        }
        else
        {
            Buckets = new byte[NumberOfBuckets];
        }

        if (NumberOfBuckets == 16) _alphaM = 0.673;
        else if (NumberOfBuckets == 32) _alphaM = 0.697;
        else if (NumberOfBuckets == 64) _alphaM = 0.709;
        else _alphaM = 0.7213 / (1 + 1.079 / NumberOfBuckets);

        _alphaMmSquared = _alphaM * NumberOfBuckets * NumberOfBuckets;
        _fiveM = 5 * NumberOfBuckets;
        _subAlgorithmSelectionThreshold = HllOptimizationHelpers.GetSubAlgorithmSelectionThreshold(precision);
    }

    #endregion

    #region Public methods

    /// <summary>
    /// Unsafe method that performs merge count.
    /// Loop unrolled.
    /// </summary>
    /// <param name="otherHll">The other hll that will be merged with this hll</param>
    /// <returns></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public unsafe long MergeCount(HllOptimizedDense_Sparse otherHll)
    {
        if (otherHll.IsSparse)
        {
            if (IsSparse)
            {
                foreach (var entry in otherHll.SparceHashSet)
                {
                    if (IsSparse)
                    {
                        if (!SparceHashSet.Contains(entry))
                        {
                            AddHash(entry);
                        }
                    }
                    else
                    {
                        //here the type changed
                        AddHash(entry);
                    }
                }

                if (IsSparse) return LastCount = SparceHashSet.Count;
                else return Count();
            } //! if (IsSparce)
            else
            {
                foreach (var entry in otherHll.SparceHashSet)
                    AddHash(entry);

                return Count();
            }
        }
        else //! if (otherHll.IsSparce)
        {
            if (IsSparse)
            {
                IsSparse = false;
                Buckets = new byte[NumberOfBuckets];
                foreach (var @ulong in SparceHashSet)
                {
                    AddHash(@ulong);
                }

                SparceHashSet = null;
            }
            else
            {
               /*no need to handle anything here because it is handled below*/
            }
        }


        double zInverse = 0;
        var vInt = 0;
        fixed (byte* bkts = Buckets)
        {
            fixed (byte* otherBkts = otherHll.Buckets)
            {
                for (var i = 0; i < NumberOfBuckets; i++)
                {
                    if (otherBkts[i] > bkts[i]) bkts[i] = otherBkts[i];
                    if (bkts[i] == 0) vInt++;
                    else zInverse += 1.0 / (1 << bkts[i]);
                }
            }
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
    public unsafe long Count()
    {
        if (IsSparse) return LastCount = SparceHashSet.Count;

        double zInverse = 0;
        int v = 0;

        fixed (byte* bkts = Buckets)
        {
            for (var i = 0; i < NumberOfBuckets; i++)
                if (bkts[i] == 0) v++;
                else zInverse += 1.0 / (1 << bkts[i]);
        }

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
    /// Adding a hash to the HLL buckets. 
    /// </summary>
    /// <param name="hash"></param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddHash(ulong hash)
    {
        TotalNumberOfInsertedElements++;
        if (IsSparse)
        {
            if (SparceHashSet.Count + 1 >= SparseCapacity)
            {
                Buckets = new byte[NumberOfBuckets];
                IsSparse = false;
                /*BUG FIX: we should not add them twice*/
                TotalNumberOfInsertedElements -= SparceHashSet.Count;
                foreach (var @ulong in SparceHashSet)
                {
                    AddHash(@ulong);
                }

                SparceHashSet = null;
            }
            else
            {
                if (!SparceHashSet.Contains(hash))
                    SparceHashSet.Add(hash);

                return;
            }
        }
       
        var bucketIdx = hash >> _bucketIndexShift;
        byte numZeros = (byte)(BitOperations.TrailingZeroCount(hash) + 1);
        if (numZeros > _bucketIndexShift)
            numZeros = _bucketIndexShift; //used to remove | BucketIndexLSBsetMask
        
        if (Buckets[bucketIdx] < numZeros)
            Buckets[bucketIdx] = numZeros;
    }

    #endregion

    public void Clean()
    {
        TotalNumberOfInsertedElements = 0;
        LastCount = 0;
        if (IsSparse)
        {
            SparceHashSet.Clear();
        }
        else
        {
            Array.Clear(Buckets, 0, NumberOfBuckets);
        }
       
    }
}