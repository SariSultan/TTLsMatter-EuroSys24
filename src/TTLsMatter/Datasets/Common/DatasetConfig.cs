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

namespace TTLsMatter.Datasets.Common;

public static class DatasetConfig
{
    /*This is important to limit the stack size for exact algorithms, otherwise the system will go out of memory*/
    public const int MaxNumberOfDistinctObjects = 600 * 1000 * 1000; /*128GB of memory 600Million*/

    /// <summary>
    /// The maximum cache size to be shown on the MRC is 2TB
    /// </summary>
    public const long MaxCacheSizeBytes = 2L * 1024 * 1024 * 1024 * 1024;
        
    /// <summary>
    /// We use stack distance histogram sizes at 32MB each (widely used in the literature to use buckets --no one use byte-level granularity)
    /// </summary>
    public const long BucketSizeBytes = 32 * 1024 * 1024;
        
    /// <summary>
    /// For uniform block size configuration, we use 4KB (widely used in the literature)
    /// </summary>
    public const int FixedBlockSize = 4096;

    /// <summary>
    /// Memcached by default limit the object size to 1MB max.
    /// IBM in their paper (Its time to revisit LRU) use a maximum object size of 4MB.
    /// We use 8MB. 
    /// </summary>
    public const uint MaxBlockSize = 8 * 1024 * 1024;
        
    /// <summary>
    /// Smallest block size is 2 bytes
    /// </summary>
    public const uint MinBlockSize = 2; 

    /// <summary>
    /// The directory for the Twitter traces formatted into binary format <see cref="TTLsMatter.Datasets.Twitter.FilteredTwitterTracesReader"/>
    /// </summary>
    public static readonly string TwitterTracesDir =@"/mnt/sari-ssd2-8tb/Traces/FilteredTwitter2/";
}