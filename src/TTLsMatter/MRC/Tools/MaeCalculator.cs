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

namespace TTLsMatter.MRC.Tools;

public static class MaeCalculator
{
    public static Tuple<double, long> ComputeMae(string refMrc, string otherMrc)
    {
        if (refMrc == null || otherMrc == null)
            return new Tuple<double, long>(1, -1);
        
        var exactDic = MrcConstructor.GetMrcDictionaryFromCsv(refMrc);
        var otherDic = MrcConstructor.GetMrcDictionaryFromCsv(otherMrc);
        
        if (exactDic == null || otherDic == null) 
            return new Tuple<double, long>(1, -1);

        if (exactDic.Count >= otherDic.Count) return ComputerMaeInternal(exactDic, otherDic);
        else return ComputerMaeInternal(otherDic, exactDic);
    }

    private static Tuple<double, long> ComputerMaeInternal(Dictionary<long, double> major,
        Dictionary<long, double> minor)
    {
        if(minor.Count==0)  return new Tuple<double, long>(1, 0);
         /*Steady state point*/
        long maxSize = minor.Max(x => x.Key);
        double minMissRatio = minor.Min(x => x.Value);

        long numberOfComparedPoints = 0;
        double diff = 0.0;

        double lastMissRatio = 1; /*BUG: Fixed Aug 13, 2023 (was set to zero)*/
        
        foreach (var entry in major)
        {
            numberOfComparedPoints++;

            if (minor.TryGetValue(entry.Key, out var value))
            {
                lastMissRatio = value;
                diff += Math.Abs(entry.Value - lastMissRatio);
            }
            else if (entry.Key > maxSize)
            {
                diff += Math.Abs(entry.Value - minMissRatio);
            }
            else
            {
                diff += Math.Abs(entry.Value - lastMissRatio);
            }
        }

        if (numberOfComparedPoints > 0)
        {
            double mae = diff / numberOfComparedPoints;
            if (mae > 1.0) mae = 1.0;
            return new Tuple<double, long>(mae, numberOfComparedPoints);
        }
        else
        {
            return new Tuple<double, long>(1, 0);
        }
    }
}