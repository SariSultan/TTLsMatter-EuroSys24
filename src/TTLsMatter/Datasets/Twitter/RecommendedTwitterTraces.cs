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

using TTLsMatter.Datasets.Common.Helpers;

namespace TTLsMatter.Datasets.Twitter;

/*
 * This class helps to get the recommended Twitter traces.
 * I did this to try to make all benchmarks follow it instead of repeating the code.
 */
public static class RecommendedTwitterTraces
{
    /*
     * Order by recommended traces
     * https://github.com/twitter/cache-trace
     */
    public static List<int> RecommendedTraceIds = new()
    {
        /*miss ratio related (admission, eviction)*/ 52, 17, 18, 24, 44, 45, 29,
        /*TTL-Related: mix of small and large TTLs*/ 52, 22, 25, 11,
        /*TTL-Related: small TTLs only*/ /*(used above)18,*/ 19, 6, 7
    };

   
    public static List<int> WriteHeavyTraceIds = new()
    {
        12, 15, 31, 37
        //MEMORY ISSUES:
        ,
        5
        ,
        21 //cluster 21 contains 0 GET accesses 
    };

    public static List<string>
        GetTraceFiles(
            string twitterMainDir /*The primary dir for the twitter binary traces */
            , bool includeWriteHeavy /*Include the write heavy traces*/
            , bool includeRecommended /*if false, all traces will be included (-+ includeWriteHeavy)*/
            , bool includeOthers /*Include the other traces (not in write-heavy and not in recommended)*/
            , List<int> includeTheseAtTop
        )
    {
        var files = new List<string>();

        if (includeOthers)
            files = Directory
                .GetFiles(twitterMainDir, "*.bin")
                .Where(x => !WriteHeavyTraceIds.Contains(TraceHelper.TwitterGetClusterId(x)))
                .Where(x => !RecommendedTraceIds.Contains(TraceHelper.TwitterGetClusterId(x)))
                .ToList();

        /*exclude heavy write workloads*/
        if (includeWriteHeavy)
            AddClusters(twitterMainDir, WriteHeavyTraceIds, files);

        /*include recommended trace*/
        if (includeRecommended)
            AddClusters(twitterMainDir, RecommendedTraceIds, files);


        /*Order by clusters id and prioritize recommended ones*/
        files = files
            .OrderBy(TraceHelper.TwitterGetClusterId)
            .ThenBy(f =>
                RecommendedTraceIds.Contains(TraceHelper.TwitterGetClusterId(f))
                    ? 0
                    : TraceHelper.TwitterGetClusterId(f))
            .ToList();


        /*Inject manually requested ones with highest priority*/
        if (includeTheseAtTop != null)
            AddClusters(twitterMainDir, includeTheseAtTop, files, true);


        files = files
            .Where(x => Path.GetFileNameWithoutExtension(x) != "cluster21") //empty cluster (has no GET requests)
            .Distinct().ToList();

        bool allFilesExist = true;
        foreach (var f in files)
        {
            if (!File.Exists(f))
            {
                allFilesExist = false;
                break;
            }
        }

        files.ForEach(f =>
        {
            if (!File.Exists(f)) throw new Exception($"FILE DOESN'T EXISTS: {f}");
        });
        if (allFilesExist)
            files = files.OrderBy(x => new FileInfo(x).Length).ToList();
        return files;
    }

    static void AddClusters(string twitterMainDir, List<int> clustersToAdd, List<string> fileList,
        bool insertAtTop = false)
    {
        foreach (var c in clustersToAdd)
        {
            var clusterPath = Path.Combine(twitterMainDir, $"cluster{c}.bin");
            if (insertAtTop) fileList.Insert(0, clusterPath);
            else fileList.Add(clusterPath);
        }
    }
}