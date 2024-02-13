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

using TTLsMatter.Datasets.Common.Types;

namespace TTLsMatter.Datasets.Common.Entities;

/// <summary>
/// All traces requests should be converted to this
/// intermediate type which is accepted across MRC algorithms
/// </summary>
public class Request
{
    /*
     * Some of the following fields might not exist in some of the traces.
     * However, for simplicity we include them (e.g., TTL, key-size)
     */
    #region BASIC FIELDS
    /// <summary>
    /// Timestamp of the access in seconds.
    /// Usually I use Unix Time with seconds precision
    /// </summary>
    public uint Timestamp;
    
    /// <summary>
    /// The 64-bit hash of the key 
    /// </summary>
    public ulong KeyHash;

    /// <summary>
    /// This is mainly added for statistics and can be safely removed for MRC
    /// processing because we use <see cref="EvictionTime"/> for that purpose
    /// which is the sum of <see cref="Ttl"/> and <see cref="Timestamp"/> when the
    /// object is added to the cache
    /// </summary>
    public uint Ttl;

    /// <summary>
    /// The time for when the request should be evicted
    /// </summary>
    public uint EvictionTime;
    
    /// <summary>
    /// The size of the key
    /// </summary>
    public uint KeySize;

    /// <summary>
    /// The size of the value
    /// </summary>
    public uint ValueSize;

    /// <summary>
    /// The request type (inherits a byte)
    /// </summary>
    public RequestType Type;
    #endregion

    #region COMPUTED PROPERTIES
    /// <summary>
    /// The <see cref="KeySize"/> + <see cref="ValueSize"/>
    /// </summary>
    public uint BlockSize => KeySize + ValueSize;
   
    #endregion


    public override string ToString()
    {
        return $"{nameof(KeyHash)}: {KeyHash}, {nameof(Timestamp)}: {Timestamp}, {nameof(KeySize)}: {KeySize}, {nameof(ValueSize)}: {ValueSize}, {nameof(Type)}: {Type}, {nameof(BlockSize)}: {BlockSize}, {nameof(EvictionTime)}: {EvictionTime}";
    }
}