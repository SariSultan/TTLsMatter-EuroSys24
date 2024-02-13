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
using TTLsMatter.Datasets.Common.Entities;

namespace TTLsMatter.Datasets.Common.Contracts
{
    /// <summary>
    /// The contract for a basic reader for DataSets in Binary Format.
    ///
    /// There might be no difference between CSV and Binary readers abstractly, but I preferred separating them.
    ///
    /// There are many benefits to work with binary formatted traces.
    /// 1. Time. It's at least 10x faster than reading and parsing CSV files.
    /// 2. Datasets are huge (e.g., Twitter), which won't fit on SSDs, hence making them binary can fit on SSDs (8TB though).
    /// 3. Seeking. You can easily seek in binary format while that's not trivial in CSV.
    /// </summary>
    public interface IBinaryDatasetReader : IDisposable
    {
        public Stopwatch TotalStopwatch { get; }
        public Stopwatch IoStopwatch { get; }

        /// <summary>
        /// The number of bytes for each request in the binary formatted trace
        /// </summary>
        public int BinaryFormattedRequestSize { get; }

        /// <summary>
        /// This will be accessed by the user.
        ///
        /// The idea is that once the reader is created, it will instantiate the array with a length equal to
        /// <see cref="BatchSize"/>.
        ///
        /// The array will keep used to read future batches which will save us time to prevent keeping allocating new memory.
        /// </summary>
        public Request[] Requests { get; set; }

        /// <summary>
        /// Defines the length of <see cref="Requests"/>
        /// </summary>
        public int BatchSize { get; }

        /// <summary>
        /// A thread-safe batch reader.
        ///
        /// Multiple readers can read at the same time.
        /// Hence, the idea is to serialize disk accesses.
        ///
        /// Two stopwatches are need to monitor performance.
        /// The totalStopwatch will stop when there is contention to disk, and starts once reading from disk starts.
        /// The totalStopwatch measures time since the beginning of the program, i.e., IO time + other processing time (e.g., MRC generation).
        /// 
        /// The ioStopwatch will start once reading from disk starts.
        /// The ioStopwatch measures IO time since the beginning of the program.
        ///
        /// For locking, I'm restricting it to the Process Domain, if inter-Process locking is needed then I recommend using the
        /// <see cref="Mutex"/> primitives instead of the compiler keyword (lock on an object).
        /// </summary>
        /// <returns>
        /// -1: when the file has ended.
        /// 0: can be returned if not all requests are needed (e.g., WRITE/READ depending on the access trace type --this does not happen with FilteredTwitter). 
        /// </returns>
        public int GetBatch();

        /// <summary>
        /// Returns number of requests in the trace.
        ///
        /// This is very easy to do in Binary formatted traces, which is the BinaryFile.Length / RequestSizeBytes
        /// </summary>
        /// <returns></returns>
        public long GetNumberOfRequests();

        /// <summary>
        /// Returns the first and last request in the trace.
        ///
        /// This is very useful to study the timespan, for instance.
        ///
        /// </summary>
        /// <returns></returns>
        public (Request, Request) GetFirstAndLastRequest(string binaryFileName);
    }
}