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

using System.Text;

namespace TTLsMatter.MRC.Tools;

public static class MrcConstructor
{
    public static Dictionary<long, double> GetMrcDictionaryFromCsv(string csvMrc)
    {
        var toRet = new Dictionary<long, double>();
        foreach (var exactLine in csvMrc.Split('\n').Where(x => !string.IsNullOrEmpty(x)).ToArray())
        {
            var parts = exactLine.Split(',');
            if (parts.Length < 2)
                return null;
            var size = long.Parse(parts[0]);
            var missRatio = double.Parse(parts[1]);
            toRet.TryAdd(size, missRatio);
        }

        return toRet;
    }


    /// <summary>
    /// Special for shards fixed size
    /// </summary>
    /// <param name="histogram"></param>
    /// <param name="numberOfRequests"></param>
    /// <param name="bucketSize"></param>
    /// <returns></returns>
    public static string GetMrcFromHistogram(double[,] histogram, long numberOfRequests, long bucketSize)
    {
        if (numberOfRequests == 0) return "0,1.000000";
        
        var histogramLength = histogram.GetLength(0);

        /*THIS MAKES SURE THAT WE DON'T OUTPUT THE SAME MIN MISS RATIO OVER AND OVER*/
        var firstNonChangingIndexExcl = 2;
        for (var i = histogramLength - 1; i > 1; i--)
            if (histogram[i, 0] != 0)
            {
                firstNonChangingIndexExcl = i + 1;
                break;
            }

        if (firstNonChangingIndexExcl > histogramLength) firstNonChangingIndexExcl = histogramLength;

        var mrc = new StringBuilder(); /*TODO:: should I provide initial capacity to reduce resizing? */
        mrc.AppendLine("0,1.000000");

        double total = 0;
        double previousTotal = 0;
        for (var i = 1; i < firstNonChangingIndexExcl; i++) /*should start from zero because the first bucket is (0,1)*/
        {
            total += histogram[i, 0];
            if (total > 0) //added to avoid dividing by zerop
                if (total >= previousTotal)
                {
                    previousTotal = total;
                    var size = i * bucketSize;
                    var missRaio = 1.000000 - (total / numberOfRequests);
                    mrc.AppendLine($"{size},{missRaio}");
                }
        }

        return mrc.ToString();
    }
    
    
    /// <summary>
    /// General usage
    /// </summary>
    /// <param name="histogram"></param>
    /// <param name="numberOfRequests"></param>
    /// <param name="bucketSize"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    public static string GetMrcFromHistogram(long[] histogram, long numberOfRequests, long bucketSize)
    {
        if (numberOfRequests == 0) return "0,1.000000";
        if (histogram == null)
        {
            throw new Exception($"in {nameof(MrcConstructor)}, histogram==null");
        }

        /*THIS MAKES SURE THAT WE DON'T OUTPUT THE SAME MIN MISS RATIO OVER AND OVER*/
        var firstNonChangingIndexExcl = 2;
        for (var i = histogram.Length - 1; i > 1; i--)
            if (histogram[i] != 0)
            {
                firstNonChangingIndexExcl = i + 1; //+1 because exclusive
                break;
            }

        if (firstNonChangingIndexExcl > histogram.Length) firstNonChangingIndexExcl = histogram.Length;

        var mrc = new StringBuilder(); /*TODO:: should I provide initial capacity to reduce resizing? */
        mrc.AppendLine("0,1.000000");

        double total = 0;
        double previousTotal = -1;
        for (var i = 1; i < firstNonChangingIndexExcl; i++)
        {
            total += histogram[i];
            if (total >= previousTotal)
            {
                previousTotal = total;
                var size = i * bucketSize;
                var missRaio = 1.0 - (total / numberOfRequests);
                mrc.AppendLine($"{size},{missRaio}");
            }
        }

        return mrc.ToString();
    }
}