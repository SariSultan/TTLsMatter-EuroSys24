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

namespace TTLsMatter.Common.Statistics;

public static class PercentileHelper
{
    /// <summary>
    /// Answer from https://stackoverflow.com/questions/8137391/percentile-calculation
    /// </summary>
    /// <param name="sequence"></param>
    /// <param name="excelPercentile"></param>
    /// <returns></returns>
    public static double Percentile(List<double> sortedList, double excelPercentile)
    {
        int N = sortedList.Count;
        double n = (N - 1) * excelPercentile + 1;
        // Another method: double n = (N + 1) * excelPercentile;
        if (n == 1d) return sortedList[0];
        else if (n == N) return sortedList[N - 1];
        else
        {
            int k = (int)n;
            double d = n - k;
            return sortedList[k - 1] + d * (sortedList[k] - sortedList[k - 1]);
        }
    }


}