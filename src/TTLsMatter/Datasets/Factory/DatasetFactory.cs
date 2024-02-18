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

using System.Diagnostics;
using TTLsMatter.Datasets.Common;
using TTLsMatter.Datasets.Common.Contracts;
using TTLsMatter.Datasets.Common.Types;
using TTLsMatter.Datasets.Twitter;

namespace TTLsMatter.Datasets.Factory;

public static class DatasetFactory
{
    public static IBinaryDatasetReader GetReader(DataSetType t, string binaryFilePath, object? iomutex = null,
        Stopwatch? ioStopwatch = null, Stopwatch? totalStopwatch = null, int batchSize = 1 * 1000 * 1000)
    {
        switch (t)
        {
            case DataSetType.FilteredTwitter:
                return new FilteredTwitterTracesReader(binaryFilePath, iomutex, ioStopwatch, totalStopwatch, batchSize);
            default:
                throw new ArgumentOutOfRangeException(nameof(t), t, null);
        }
    }

    public static List<string> GetTraceFiles(DataSetType t)
    {
        switch (t)
        {
            case DataSetType.FilteredTwitter:
                if (Program.SINGLE_TRACE_MODE)
                {
                    return new List<string>()
                    {
                        Program.SINGLE_TRACE_PATH
                    };
                }
                
                return RecommendedTwitterTraces.GetTraceFiles(DatasetConfig.TwitterTracesDir, false, false, false,
                    new List<int>()
                    {
                        /*The 28 workloads we use in our study Sultan et al. TTLs Matter - EuroSys'24*/
                         4,  6,  7,  8, 13, 
                        14, 16, 18, 19, 22, 
                        24, 29, 30, 33, 34, 
                        37, 40, 41, 42, 43, 
                        46, 48, 49, 50, 52,
                        54, 25, 11
                    }
                );
            default:
                throw new ArgumentOutOfRangeException(nameof(t), t, null);
        }
    }

    public static string GetTraceFileName(string filePath, DataSetType dst)
    {
        return Path.GetFileNameWithoutExtension(filePath);
    }
}