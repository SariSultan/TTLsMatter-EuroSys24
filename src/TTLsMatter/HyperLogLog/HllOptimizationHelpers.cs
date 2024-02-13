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

namespace TTLsMatter.HyperLogLog;

public static class HllOptimizationHelpers
{

    /// <summary>
    ///     Returns the threshold determining whether to use LinearCounting or HyperLogLog for an estimate. Values are from the supplementary
    ///     material of Huele et al.,
    ///     <see cref="http://docs.google.com/document/d/1gyjfMHy43U9OWBXxfaeG-3MjGzejW1dlpyMwEYAAWEI/view?fullscreen#heading=h.nd379k1fxnux" />
    /// </summary>
    /// <param name="bits">Number of bits</param>
    /// <returns></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double GetSubAlgorithmSelectionThreshold(int bits)
    {
        switch (bits)
        {
            case 4:
                return 10;
            case 5:
                return 20;
            case 6:
                return 40;
            case 7:
                return 80;
            case 8:
                return 220;
            case 9:
                return 400;
            case 10:
                return 900;
            case 11:
                return 1800;
            case 12:
                return 3100;
            case 13:
                return 6500;
            case 14:
                return 11500;
            case 15:
                return 20000;
            case 16:
                return 50000;
            case 17:
                return 120000;
            case 18:
                return 350000;
        }
        throw new ArgumentOutOfRangeException("bits", "Unexpected number of bits (should never happen)");
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte GetNumberOfZerosLikeRedis(ulong hash, byte precision)
    {
        byte count = 1;
        for (int i = 0; i < 64 - precision; i++)
        {
            if (((hash >> i) & 1) == 0) ++count;
            else break;
        }

        return count;
    }
}