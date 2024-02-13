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

namespace TTLsMatter.MRC.Common;

public enum MrcGenerationAlgoType : byte
{
    /* FOR REPORTING, I'M MAKING NO TTL START 0-127 AND TTL 128-255*/
    /*EXACT range*/
    OlkenNoTtl = 1,

    /*Shards Fixed Rate*/
    ShardsFixedRateNoTtlPoint1=2,
    ShardsFixedRateNoTtlPointZero1=3,
    ShardsFixedRateNoTtlPointZeroZero1=4,

    ShardsAdjFixedRateNoTtlPoint1=5,
    ShardsAdjFixedRateNoTtlPointZero1=6,
    ShardsAdjFixedRateNoTtlPointZeroZero1=7,

    /*Shards Fixed Space*/
    ShardsAdjFixedSpaceNoTtl1K=8,
    ShardsAdjFixedSpaceNoTtl2K=9,
    ShardsAdjFixedSpaceNoTtl4K=10,
    ShardsAdjFixedSpaceNoTtl8K=11,
    ShardsAdjFixedSpaceNoTtl16K=12,
    ShardsAdjFixedSpaceNoTtl32K=13,
    ShardsAdjFixedSpaceNoTtl64K=14,

    /*CounterStacks*/
    CounterStacksHiFiP16NoTtl=15,
    CounterStacksLoFiP16NoTtl=16,
    CounterStacksHiFiP12NoTtl=17,
    CounterStacksLoFiP12NoTtl=18,
    
  

/*============================ [ TTL STUFF ] ==============================*/
    OlkenTtl=19,

    ShardsFixedRateTtlPoint1=20,
    ShardsFixedRateTtlPointZero1=21,
    ShardsFixedRateTtlPointZeroZero1=22,
    
    ShardsAdjFixedRateTtlPoint1=23,
    ShardsAdjFixedRateTtlPointZero1=24,
    ShardsAdjFixedRateTtlPointZeroZero1=25,
    
    ShardsAdjFixedSpaceTtl1K=26,
    ShardsAdjFixedSpaceTtl2K=27,
    ShardsAdjFixedSpaceTtl4K=28,
    ShardsAdjFixedSpaceTtl8K=29,
    ShardsAdjFixedSpaceTtl16K=30,
    ShardsAdjFixedSpaceTtl32K=31,
    ShardsAdjFixedSpaceTtl64K=32,
    
    
    CounterstacksPlusPlusTtlFbs50CountersP12T8HiFi=33,
    CounterstacksPlusPlusTtlFbs50CountersP12T8LoFi=34,
    
    CounterstacksPlusPlusTtlFbs100CountersP12T8HiFi=35,
    CounterstacksPlusPlusTtlFbs100CountersP12T8LoFi=36,
    
    CounterstacksPlusPlusTtlFbs200CountersP12T8HiFi=37,
    CounterstacksPlusPlusTtlFbs200CountersP12T8LoFi=38,
    
}