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

namespace TTLsMatter.WSS.Common;

public interface IWssCalculator
{
    public long NumberOfInserts { get; set; }
    public void AddRequest(Request request);
    public void AddRequests(Request[] requests, int startIdx, int endIdxExclusive);

    public long GetWss_FixedBlockSize(uint currentTime);
    public long GetWss_VariableBlockSize(uint currentTime);
    public long GetWss_VariableBlockSize_RunningAverage(uint currentTime);
    public long GetCardinality(uint currentTime);

    /// <summary>
    /// Resets the estimator
    /// </summary>
    public void Clean();

    /// <summary>
    /// Dense static serialization, where the internal structures of the HLL are serialized.
    /// </summary>
    /// <returns>
    /// Exact WSS calculator: null
    /// HLL estimator : the serialized object according the to implementation
    /// </returns>
    public byte[] SerializeStatic();

    /// <summary>
    /// Sparse Dynamic serialization 
    /// </summary>
    /// <returns>
    /// Exact WSS calculator: null
    /// HLL estimator: the serialized object according to the implementation
    /// </returns>
    public byte[] SerializeDynamic();

    /// <summary>
    /// 
    /// </summary>
    /// <param name="serializedObject"></param>
    /// <returns>
    /// 0: success
    /// else: error code
    ///        1: cannot be deserialized because 
    /// </returns>
    public int Deserialize(byte[] serializedObject);

    public void Merge(IWssCalculator other);
}