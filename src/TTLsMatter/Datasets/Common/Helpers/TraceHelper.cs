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

namespace TTLsMatter.Datasets.Common.Helpers;

public static class TraceHelper
{
    public static int TwitterGetClusterId(string fullPath)
    {
        return int.Parse(Path.GetFileNameWithoutExtension(fullPath).ToLower().Replace("cluster", ""));
    }
}