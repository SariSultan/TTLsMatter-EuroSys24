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

using TTLsMatter.Datasets.Common;
using TTLsMatter.MRC.Common;
using TTLsMatter.MRC.Counterstacks;
using TTLsMatter.MRC.CounterstacksPlusPlus;
using TTLsMatter.MRC.Shards;
using TTLsMatter.MRC.ShardsPlusPlus;

namespace TTLsMatter.MRC.Factory;

public static class MrcAlgorithmFactory
{
    /// <summary>
    /// Returns an instance of an algorithms with specific configuration.
    ///
    /// </summary>
    /// <param name="algorithm"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public static IMrcGenerationAlgorithm GetAlgorithm(MrcGenerationAlgoType algorithm)
    {
        switch (algorithm)
        {
            case MrcGenerationAlgoType.OlkenNoTtl:
                return new Olken.Olken(DatasetConfig.MaxCacheSizeBytes, DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.OlkenTtl:
                return new OlkenPlusPlus.OlkenPlusPlus(DatasetConfig.MaxCacheSizeBytes, DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);

            case MrcGenerationAlgoType.ShardsFixedRateNoTtlPoint1:
                return new FixedRateShards(0.1, false, DatasetConfig.MaxCacheSizeBytes, DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedRateNoTtlPoint1:
                return new FixedRateShards(0.1, true, DatasetConfig.MaxCacheSizeBytes, DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsFixedRateNoTtlPointZero1:
                return new FixedRateShards(0.01, false, DatasetConfig.MaxCacheSizeBytes, DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedRateNoTtlPointZero1:
                return new FixedRateShards(0.01, true, DatasetConfig.MaxCacheSizeBytes, DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsFixedRateNoTtlPointZeroZero1:
                return new FixedRateShards(0.001, false, DatasetConfig.MaxCacheSizeBytes, DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedRateNoTtlPointZeroZero1:
                return new FixedRateShards(0.001, true, DatasetConfig.MaxCacheSizeBytes, DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsFixedRateTtlPoint1:
                return new FixedRateShardsPlusPlus(0.1, false, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedRateTtlPoint1:
                return new FixedRateShardsPlusPlus(0.1, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsFixedRateTtlPointZero1:
                return new FixedRateShardsPlusPlus(0.01, false, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedRateTtlPointZero1:
                return new FixedRateShardsPlusPlus(0.01, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsFixedRateTtlPointZeroZero1:
                return new FixedRateShardsPlusPlus(0.001, false, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedRateTtlPointZeroZero1:
                return new FixedRateShardsPlusPlus(0.001, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl1K:
                return new FixedSizeShards(1 * 1000, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl2K:
                return new FixedSizeShards(2 * 1000, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl4K:
                return new FixedSizeShards(4 * 1000, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl8K:
                return new FixedSizeShards(8 * 1000, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl16K:
                return new FixedSizeShards(16 * 1000, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl32K:
                return new FixedSizeShards(32 * 1000, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedSpaceNoTtl64K:
                return new FixedSizeShards(64 * 1000, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl1K:
                return new FixedSizeShardsPlusPlus(1* 1000, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl2K:
                return new FixedSizeShardsPlusPlus(2 * 1000, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl4K:
                return new FixedSizeShardsPlusPlus(4 * 1000, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl8K:
                return new FixedSizeShardsPlusPlus(8 * 1000, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl16K:
                return new FixedSizeShardsPlusPlus(16 * 1000, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl32K:
                return new FixedSizeShardsPlusPlus(32 * 1000, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.ShardsAdjFixedSpaceTtl64K:
                return new FixedSizeShardsPlusPlus(64 * 1000, true, DatasetConfig.MaxCacheSizeBytes,
                    DatasetConfig.BucketSizeBytes,
                    DatasetConfig.FixedBlockSize);
            case MrcGenerationAlgoType.CounterStacksHiFiP16NoTtl:
                return new CounterStacks(16, DatasetConfig.FixedBlockSize, 
                    DatasetConfig.MaxCacheSizeBytes, true, DatasetConfig.BucketSizeBytes);
            case MrcGenerationAlgoType.CounterStacksLoFiP16NoTtl:
                return new CounterStacks(16, DatasetConfig.FixedBlockSize, 
                    DatasetConfig.MaxCacheSizeBytes, false, DatasetConfig.BucketSizeBytes);
            case MrcGenerationAlgoType.CounterStacksHiFiP12NoTtl:
                return new CounterStacks(12, DatasetConfig.FixedBlockSize, 
                    DatasetConfig.MaxCacheSizeBytes, true, DatasetConfig.BucketSizeBytes);
            case MrcGenerationAlgoType.CounterStacksLoFiP12NoTtl:
                return new CounterStacks(12, DatasetConfig.FixedBlockSize, 
                    DatasetConfig.MaxCacheSizeBytes, false, DatasetConfig.BucketSizeBytes);
              
            case MrcGenerationAlgoType.CounterstacksPlusPlusTtlFbs50CountersP12T8HiFi:
                return new CounterstacksPlusPlusFbsTtl(12, 8, DatasetConfig.FixedBlockSize,
                    DatasetConfig.MaxCacheSizeBytes, DatasetConfig.BucketSizeBytes, 50, 30, true);
            case MrcGenerationAlgoType.CounterstacksPlusPlusTtlFbs50CountersP12T8LoFi:
                return new CounterstacksPlusPlusFbsTtl(12, 8, DatasetConfig.FixedBlockSize,
                    DatasetConfig.MaxCacheSizeBytes, DatasetConfig.BucketSizeBytes, 50, 60, false);

            case MrcGenerationAlgoType.CounterstacksPlusPlusTtlFbs100CountersP12T8HiFi:
                return new CounterstacksPlusPlusFbsTtl(12, 8, DatasetConfig.FixedBlockSize,
                    DatasetConfig.MaxCacheSizeBytes, DatasetConfig.BucketSizeBytes, 100, 30, true);
            case MrcGenerationAlgoType.CounterstacksPlusPlusTtlFbs100CountersP12T8LoFi:
                return new CounterstacksPlusPlusFbsTtl(12, 8, DatasetConfig.FixedBlockSize,
                    DatasetConfig.MaxCacheSizeBytes, DatasetConfig.BucketSizeBytes, 100, 60, false);
          
            case MrcGenerationAlgoType.CounterstacksPlusPlusTtlFbs200CountersP12T8HiFi:
                return new CounterstacksPlusPlusFbsTtl(12, 8, DatasetConfig.FixedBlockSize,
                    DatasetConfig.MaxCacheSizeBytes, DatasetConfig.BucketSizeBytes, 200, 30, true);
            case MrcGenerationAlgoType.CounterstacksPlusPlusTtlFbs200CountersP12T8LoFi:
                return new CounterstacksPlusPlusFbsTtl(12, 8, DatasetConfig.FixedBlockSize,
                    DatasetConfig.MaxCacheSizeBytes, DatasetConfig.BucketSizeBytes, 200, 60, false);
            
            default:
                throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, null);
        }
    }
}