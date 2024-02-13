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

using LinqStatistics;

namespace TTLsMatter.Common.Statistics;

public static class ConfidenceIntervalHelper
{
    /// <summary>
    /// https://docs.microsoft.com/en-us/office/vba/api/excel.worksheetfunction.confidence
    /// </summary>
    /// <returns></returns>
    public static double Get95Confidence(List<double> samples)
    {
       // throw new Exception("NEEDS SDK8.0");
        if (samples.Count <= 1) return 0;
        double confidenceInterval95 = 1.96 * samples.StandardDeviation() / Math.Sqrt(samples.Count);
        return confidenceInterval95;
    }
}