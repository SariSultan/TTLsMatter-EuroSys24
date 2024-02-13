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

namespace TTLsMatter.Datasets.Common.StreamReader
{
    public static class FileStreamHelper
    {
        public static int ReadChunk(
            FileStream reader
            , int batchSize
            , int binaryFormattedRequestSize
            , byte[] buffer
            , Stopwatch ioStopwatch
            , Stopwatch totalStopwatch
            , object ioMutex)
        {
            /*impossible case*/
            if (reader.Position > reader.Length)
                throw new Exception($"Impossible case in {nameof(FileStreamHelper)}. " +
                                    $"The binary reader exceeded the position of the base stream. (must be a bug)");

            /*if the file has ended*/
            if (reader.Position == reader.Length) return -1;

            int items = reader.Position + batchSize * binaryFormattedRequestSize < reader.Length
                ? batchSize
                : (int)((reader.Length - reader.Position) / binaryFormattedRequestSize);

            int bytesToRead = items * binaryFormattedRequestSize;

            totalStopwatch.Stop();
            lock (ioMutex)
            {
                totalStopwatch.Start();
                ioStopwatch.Start();
                if (bytesToRead > buffer.Length)
                    throw new Exception($"In {nameof(ReadChunk)} bytesToRead>buffer.Length");
                int bytesRead = 0;
                try
                {
                    bytesRead = reader.Read(buffer, 0, bytesToRead);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(
                        $"EXCEPTION IN {nameof(FileStreamHelper)}.{nameof(ReadChunk)}-read() syscall. EX:{ex.ToString()}");
                    throw;
                }

                if (bytesToRead != bytesRead)
                    throw new Exception(
                        $"{nameof(FileStreamHelper)}. Expected to read {bytesToRead} bytes but only read {bytesRead}.");

                ioStopwatch.Stop();
            }

            if (!totalStopwatch.IsRunning)
                throw new Exception("totalStopwatch should be enabled here.");

            return items;
        }
    }
}