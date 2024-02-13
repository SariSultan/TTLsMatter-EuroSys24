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
using System.Security.Cryptography;

namespace TTLsMatter.Common.Hashing;

public static class HashingHelper
{
    #region murmur Hashing 
    /// <summary>
    ///   I ported Redis implementation in C to C# and made it always big endian
    ///
    ///   Redis implementation: http://download.redis.io/redis-stable/src/hyperloglog.c
    /// </summary>
    /// <param name="k"></param>
    /// <returns></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong MurmurHash264A(UInt64 k)
    {
        const ulong m = 0xc6a4a7935bd1e995;
        ulong h = 3829533692205168561; //seed ^ (len * m);
        k *= m;
        k ^= k >> 47;
        k *= m;
        h ^= k;
        h *= m;

        h ^= h >> 47;
        h *= m;
        h ^= h >> 47;
        return h;
    }
    #endregion Hashing
    
    
    #region SHA-1
    public static string ComputeSHA1Hash(string filePath)
    {
        // Ensure the file exists
        if (!File.Exists(filePath))
        {
            return ($"File not found: {filePath}");
        }

        using FileStream fileStream = File.OpenRead(filePath);
        using SHA1 sha1 = SHA1.Create();
        
        // Compute the hash of the file
        byte[] hashBytes = sha1.ComputeHash(fileStream);

        // Convert the byte array to a hex string
        return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
    }
    #endregion SHA-1
}