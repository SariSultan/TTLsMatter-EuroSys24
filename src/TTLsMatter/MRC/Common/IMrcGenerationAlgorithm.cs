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

using TTLsMatter.Datasets.Common.Entities;

namespace TTLsMatter.MRC.Common;

/// <summary>
/// Inherited by all MRC Generation Algorithms
///
/// (C) Sari Sultan, sarisultan@ieee.org
/// </summary>
public interface IMrcGenerationAlgorithm
{
    public void AddRequest(Request request);
    public void AddRequests(Request[] requests, int startIdx, int endIdxExclusive);
    
    public string GetMrc_FixedBlockSize(); 
    public string GetMrc_VariableBlockSize_RunningAverage(); 
}