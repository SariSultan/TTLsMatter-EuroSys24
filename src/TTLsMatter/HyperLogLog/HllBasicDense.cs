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
using TTLsMatter.Common.Hashing;

namespace TTLsMatter.HyperLogLog;

[Serializable]
public class HllBasicDense
{
    public uint CREATION_TIMESTAMP = 0;

    public HllBasicDense Clone()
    {
        var newHll = new HllBasicDense(Precision);
        Array.Copy(Buckets, newHll.Buckets, Buckets.Length);
        newHll.LastCount = LastCount;
        return newHll;
    }

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
    public readonly byte Precision;

    /// <summary>
    /// Number of buckets = 2^ <see cref="Precision"/>
    /// </summary>
    public readonly int NumberOfBuckets;

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
    public int BlockSize;

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
    public HllBasicDense(byte precision, int id = 0)
    {
        if (precision < 4 || precision > 16)
        {
            throw new Exception("Hyperloglog initialization error." +
                                "Precision should be in [4,16], " +
                                $"the value {precision} was provided instead.");
        }

        Id = id;
        NumberOfUsers = 0;
        Buckets = new byte[1 << precision];

        Precision = precision;
        NumberOfBuckets = 1 << precision;
        _bucketIndexShift = (byte)(64 - precision);
        _bucketIndexLsbSetMask = 1UL << _bucketIndexShift;

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
    /// Merging without our optimized count operation
    /// </summary>
    /// <param name="otherHll"></param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Merge(HllBasicDense otherHll)
    {
        for (var i = 0; i < NumberOfBuckets; i++)
            if (otherHll.Buckets[i] > Buckets[i])
                Buckets[i] = otherHll.Buckets[i];
    }

    /// <summary>
    /// Perform HLL count operation
    /// </summary>
    /// <returns>count</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Count()
    {
        double zInverse = 0;
        int v = 0;

        for (var i = 0; i < NumberOfBuckets; i++)
            if (Buckets[i] == 0) v++;
            else zInverse += 1.0 / (1 << Buckets[i]);

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
    /// This methods adds a key to the HLL.
    /// It uses the default hash algorithm which is Murmur2A 64-bits
    /// </summary>
    /// <param name="item">the key</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddItem(ulong item)
    {
        var hash = HashingHelper.MurmurHash264A(item);
        //finding the bucket index
        var bucketIdx = hash >> _bucketIndexShift;
        var currentBucketValue = Buckets[bucketIdx];

        var numZeros = HllOptimizationHelpers.GetNumberOfZerosLikeRedis(hash, Precision);
        if (currentBucketValue < numZeros)
        {
            Buckets[bucketIdx] = numZeros;
        }
    }

    /// <summary>
    /// Adding a hash to the HLL buckets. 
    /// </summary>
    /// <param name="hash"></param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddHash(ulong hash)
    {
        //finding the bucket index
        var bucketIdx = hash >> _bucketIndexShift;
        var currentBucketValue = Buckets[bucketIdx];
        var numZeros = HllOptimizationHelpers.GetNumberOfZerosLikeRedis(hash, Precision);
        if (currentBucketValue < numZeros)
        {
            Buckets[bucketIdx] = numZeros;
        }
    }

    #endregion
}