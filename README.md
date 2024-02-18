# TTLs Matter: Efficient Cache Sizing with TTL-Aware Miss Ratio Curves and Working Set Sizes (EuroSys'24)

This guide is designed to assist you installing and executing our simulations on your workstation. We provide links to the access traces used in our evaluation section as well. 

## Abstract 
In-memory caches play a pivotal role in optimizing distributed systems by significantly reducing query response times. Correctly sizing these caches is critical, especially considering that prominent organizations use terabytes and even petabytes of DRAM for these caches. 
The Miss Ratio Curve (MRC) and Working Set Size (WSS) are the most widely used tools for sizing these caches. 

Modern cache workloads employ Time-to-Live (TTL) limits to define the lifespan of cached objects, a feature essential for ensuring data freshness and adhering to regulations like GDPR. Surprisingly, none of the existing MRC and WSS tools accommodate TTLs. 
Based on $28$ real-world cache workloads that contain $113$ billion accesses, we show that taking TTL limits into consideration allows a $69\%$ lower memory footprint for in-memory caches on average (and up to $99\%$) without a degradation in the hit rate. 

This paper describes how TTLs can be integrated into today's most important MRC generation and WSS estimation algorithms. 
We also describe how the widely used HyperLogLog (HLL) cardinality estimator can be extended to accommodate TTLs, and show how it can be used to efficiently estimate the WSS. 
Our extended algorithms maintain comparable performance levels to the original algorithms. 
All our extended approximate algorithms are efficient, run in constant space, and enable more resource-efficient and cost-effective cache management.

## Dependencies

Although the code is cross-platform and supports most operating systems including Linux, Windows, and macOS, for the artifact evaluation, we assume Ubuntu is used (the specific version we used is 23.04). 

### 1/2 dotnet sdk `v8.0.100`
Following are the commands you need to execute to download and install the sdk on Ubuntu. 
* `wget https://download.visualstudio.microsoft.com/download/pr/5226a5fa-8c0b-474f-b79a-8984ad7c5beb/3113ccbf789c9fd29972835f0f334b7a/dotnet-sdk-8.0.100-linux-x64.tar.gz`

* `mkdir -p $HOME/dotnet && tar zxf dotnet-sdk-8.0.100-linux-x64.tar.gz -C $HOME/dotnet`

* `export DOTNET_ROOT=$HOME/dotnet`  [note: better to add to .bashrc]

* `export PATH=$PATH:$HOME/dotnet`	[note: better to add to .bashrc]

### 2/2 Gnuplot
We have used v5.4 which is the default installed with 'apt install'. Any version should be fine. This is only used to generate the plots automatically when running the simulations. To install gnuplot on your system, execute the command:
* `sudo apt install gnuplot` 


## Compilation (Ubuntu - linux-x64)

If you are using Ubuntu, follow the two steps below to compile the code:
1. go to `src/TTLsMatter` directory 
2. run `./publish.sh`

This will create a compiled binary called `TTLsMatter` inside `src/TTLsMatter/bin` directory. 

### Compilation for other operating systems
The `publish.sh` file includes a single command: `dotnet publish --configuration Release --property WarningLevel=0 --runtime linux-x64 -p:PublishSingleFile=true --self-contained false -p:DebugType=None -p:IncludeSymbols=false -o "bin"` 
For other operating systems you can change the runtime identifier. For Windows you can change it to `--runtime win-x64`, and for macOS you can change it to `--runtime osx-x64`.

## Access Traces 

### Download links

We have hosted the $28$ access traces used in our evaluation section. They can be downloaded from Google storage cloud using the following links. I will maintain this for at least $1$ year (i.e., until Feb 2025), funded by myself (any sponsor who would like to host them is appreciated).

We use `zstd` to compress the access traces (`sudo apt install zstd`). To decompress an access trace use `zstd -d clusterX.zstd -o clusterX.bin`, where `X` is the cluster id. 

These $28$ clusters require $2.7$TB of space when decompressed and $700$GB of space in their compressed format. Next to each cluster, listed below, we put its size in compressed format.
For testing purposes we recommend using the smallest cluster which uses `700MB` in compressed format and `1.3GB` when decompressed: you can download the decompressed cluster50 from [here](https://storage.googleapis.com/filteredtwitter2/cluster50.bin). 

[`cluster4` (16GB)](https://storage.googleapis.com/filteredtwitter2/cluster4.zst)
[`cluster6` (51GB)](https://storage.googleapis.com/filteredtwitter2/cluster6.zst)
[`cluster7` (4GB)](https://storage.googleapis.com/filteredtwitter2/cluster7.zst)
[`cluster8` (7GB)](https://storage.googleapis.com/filteredtwitter2/cluster8.zst)
[`cluster11` (22GB)](https://storage.googleapis.com/filteredtwitter2/cluster11.zst)

[`cluster13` (2GB)](https://storage.googleapis.com/filteredtwitter2/cluster13.zst)
[`cluster14` (10GB)](https://storage.googleapis.com/filteredtwitter2/cluster14.zst)
[`cluster16` (55GB)](https://storage.googleapis.com/filteredtwitter2/cluster16.zst)
[`cluster18` (65GB)](https://storage.googleapis.com/filteredtwitter2/cluster18.zst)
[`cluster19` (17GB)](https://storage.googleapis.com/filteredtwitter2/cluster19.zst)

[`cluster22` (3GB)](https://storage.googleapis.com/filteredtwitter2/cluster22.zst)
[`cluster24` (15GB)](https://storage.googleapis.com/filteredtwitter2/cluster24.zst)
[`cluster25` (11GB)](https://storage.googleapis.com/filteredtwitter2/cluster25.zst) 
[`cluster29` (38GB)](https://storage.googleapis.com/filteredtwitter2/cluster29.zst)
[`cluster30` (12GB)](https://storage.googleapis.com/filteredtwitter2/cluster30.zst)

[`cluster33` (27GB)](https://storage.googleapis.com/filteredtwitter2/cluster33.zst)
[`cluster34` (17GB)](https://storage.googleapis.com/filteredtwitter2/cluster34.zst)
[`cluster37` (14GB)](https://storage.googleapis.com/filteredtwitter2/cluster37.zst)
[`cluster40` (19GB)](https://storage.googleapis.com/filteredtwitter2/cluster40.zst)
[`cluster41` (16GB)](https://storage.googleapis.com/filteredtwitter2/cluster41.zst)

[`cluster42` (15GB)](https://storage.googleapis.com/filteredtwitter2/cluster42.zst)
[`cluster43` (52GB)](https://storage.googleapis.com/filteredtwitter2/cluster43.zst)              
[`cluster46` (24GB)](https://storage.googleapis.com/filteredtwitter2/cluster46.zst)
[`cluster48` (9GB)](https://storage.googleapis.com/filteredtwitter2/cluster48.zst)
[`cluster49` (3GB)](https://storage.googleapis.com/filteredtwitter2/cluster49.zst)

[`cluster50` (600MB)](https://storage.googleapis.com/filteredtwitter2/cluster50.zst)
[`cluster52` (77GB)](https://storage.googleapis.com/filteredtwitter2/cluster52.zst)                     
[`cluster54` (122GB)](https://storage.googleapis.com/filteredtwitter2/cluster54.zst)  
 
 
### Access Traces Format

Our access traces are binary formatted using the following format, and sorted by timestamp.

| Property  | Type                           |
| --------- | ------------------------------ |
| Timestamp | Time in Seconds (uint32)       |
| Key       | uint64                         |
| Size      | uint32                         |
| Eviction Time       | uint32 (TTL + Timestamp) |

Each access thus requires 20 bytes. The binary format reader and format details are documented in `src/TTLsMatter/Datasets/Twitter/FilteredTwitterTracesReader.cs`

* _Alternatively_, the raw version of the Twitter workloads can be obtained from https://github.com/twitter/cache-trace
-- We downloaded the CSV version from SNIA, which require approximately 14TB for the decompressed CSV files. We then formatted them into binary following the process detailed in `src/TTLsMatterDatasets/Twitter/FilteredTwitterTracesReader.cs` (this is a time-consuming process, takes approximately a week)

## Reproducing the evaluation section results 
The process of reproducing results often involves a complex series of steps: generating the initial results, processing these results into CSV files, and finally executing plotting scripts to produce the necessary figures.

Since we test a large set of algorithms and configurations, we have dedicated considerable effort to streamline this process, ensuring that you can replicate the results from our evaluation section (specifically, Figures 14, 15, 16, 17A, 17B, and 18) with a single command.

To do so, after compiling the code, simply execute `./src/TTLsMatter/bin/TTLsMatter` (it is better to execute it in the `bin` working directory (using `./TTLsMatter`) as it will create new subdirectories there). Upon completion, you will find all of the generated plots within a new directory called `Figures`, ready for your review.

The process to replicate the full results for the $28$ access traces, takes $4.8$ days. Thus, as per the artificat evaluation guidelines which states:
>"Screencast: A detailed screencast of the tool along with the results can be an option if one of the following special cases applies: [...] The artifact requires significant computation resources (e.g., more than 24 hours of execution time to produce the results) or requires huge data sets.", 

Hence, we have provided a screencast of the full process, which is $4.8$ days long. The video starts by cloning this repository, compiling the code, and running the compiled binary. We instrumented the code to generate the SHA-1 hash of every file produced by the code to maintain its integrity. 

In the last $10$ minutes of the video, after the the program concludes, we open the generated figures, compress all the generated results and generate the SHA-1 hash of the compressed files. We also use `nohup` to redirect the output of the program while it is running, similarly we provide the SHA-1 hash of the `nohup` output as well. Lastly, to verify the integrity of the video, we make a copy of the video towards the end while the screen is still recording and provide the SHA-1 hash of that copied video file. Then, we provide the complete video again which shows the copying and hashing process. The links for the video file and compressed output are as follows:

| Download Link  | Description                           |
| --------- | ------------------------------ |
| [TTLsMatter-EuroSys24.zip (100MB)](https://storage.googleapis.com/filteredtwitter2/TTLsMatter-EuroSys24.zip)   | An archive that contains all of the generated results, including the `nohup` file |
| [TTLsMatter-EuroSys24.mkv (13GB)](https://storage.googleapis.com/filteredtwitter2/TTLsMatter-EuroSys24.mkv) | The full video copy that we made while recording the experiment to be able to generate the SHA-1 hash of the full video process.|
|[2024-02-13 2007-48-26.mkv (13GB)](https://storage.googleapis.com/filteredtwitter2/2024-02-13%2007-48-26.mkv)| The full video which shows the full process from the start to the end including making and hashing of `TTLsMatter-EuroSys24.mkv` and `TTLsMatter-EuroSys24.zip`| 
|[last 10 minutes](https://storage.googleapis.com/filteredtwitter2/last_10_mins.mp4) | The last 10 minutes of the full video which shows the generated figures, the hashing process of `TTLsMatter-EuroSys24.zip` and `TTLsMatter-EuroSys24.mkv`|

We ran this experiment on a server equipped with Intel-13900KS, 128GB of 4800MHz DRAM (you need at least 128 GB of memory to process all the access traces, otherwise the system will go out of memory), and the access traces were read from 8TB NVME SSD.

### Testing a subset of the  traces
Waiting almost $5$ days is not practical, thus to test smaller subset of the $28$ workloads, we provide two methods as follows: 

- *Method 1* Running a single access trace: 

    -- After downloading the access trace you like, you run the compiled binary file and provide the path to the trace as an argument. This will generate all the results for all of our algorithms for that trace. (We describe the outputted results further below). 
    
    --For example, if you use the smallest access trace (cluster50, download [here]([here](https://storage.googleapis.com/filteredtwitter2/cluster50.bin))), then you run `./TTLsMatter <path_to_cluster50.bin>`. For cluster50, this process is supposed to take less than an hour and does not require a lot of memory ($16$ GB of DRAM should be fine). 

- *Method 2* Processing a subset of the traces: 

    1. After downloading the formatted access traces and decompress them (make sure each file ends with '.bin', e.g., cluster4.bin), you need update the configuration of the code to point to the directory that contains the traces. To do so, go to  `src/TTLsMatter/Datasets/Common/DatasetConfig.cs` and change the variable *TwitterTracesDir* value to be the absolute path of the directory that contains the access traces. 

    ```csharp
    //src/TTLsMatter/Datasets/Common/DatasetConfig.cs
    public static readonly string TwitterTracesDir =@"PATH_TO_THE_TRACES_DIR";
    ```

    2. Change the configuration of the subset of traces you want to run by going to `src/TTLsMatter/Datasets/Factory/DatasetFactory.cs` and update the cluster ids in `GetTraceFiles` to the desired ones. These clusters should exist in the path you provided otherwise an error would be raised when the program starts to warn you that it is missing. 

    ```csharp 
    //src/TTLsMatter/Datasets/Factory/DatasetFactory.cs
    return RecommendedTwitterTraces
        .GetTraceFiles(DatasetConfig.TwitterTracesDir
        , false, false, false
        ,  new List<int>()
    {
        /*Select the subset of trace, for example below we only select cluster50 and comment the others*/
        
        50
        /*4,  6,  7,  8, 13, 
        14, 16, 18, 19, 22, 
        24, 29, 30, 33, 34, 
        37, 40, 41, 42, 43, 
        46, 48, 49, 50, 52,
        54, 25, 11*/
    }
    );
    ```

### Description of the output directories

After you run `./TTLsMatter` and it finishes executing, the results will be generated in new directories created in the working directory. Following is a description of each of the generated directories. 

* `WSS-AE-2024` This directory contains the Working Set Size (WSS) results using the presented algorithms in the paper. Each access trace will have its own subdirectory `WSS-AE-2024/clusterX` where X is the cluster ID. A CSV file will be created for each algorithm/configuration. Each CSV contains the WSS of the workload measured by the end of of each hour of the duration of the workload. For instance, the file `WSS-AE-2024/cluster50/cluster50-WSS-HLL-b16-TTL.csv` will contain the WSS results using our proposed HLL-TTL using a precision parameter b=16.
In addition, the main directory `WSS-AE-2024` will also include the aggregate statistics for all of the access traces. This includes $7$ csv files that are automatically generated after all the access traces are processed. These CSV files are necessary for the plotting script. 

* `MRCs-AE-2024` This directory contains the MRCs for each of the algorithms we present in our paper. Organized by `MRCs-AE-2024/<Dataset Name>/<Algorithm Name>/<Object Size Config [fbs|runningAvg]>/`. We are mainly interested in the `runningAvg` directory for each algorithm. This directory contains two files per access trace. 

    -- `clusterX.csv` this is the generated MRC (where `X` is the cluster ID). The MRC is a set of <cache size, miss ratio> tuples. Thus, each MRC .csv file has two columns the cache size and miss ratio. 
    
    -- `clusterX.stat`this is the statistics for that access trace which includes   (TraceName, Dataset, Algorithm, NumberOfLinesProcessed, IOTime(s), ProcessingTimeWithoutIO(s), TotalTime(s), ProcessingSpeed(r/s), OverallSpeedWithIO(r/s)). 

* After the code generates all the CSVs for all the access traces, the code automatically generates the aggregate statistics for the generated results to be used by the plotting scripts to generate the MRC-related figures. Thus $3$ directories will be automatically created, namely, `MRCs-AE-2024-plots-data-mad`, `MRCs-AE-2024-plots-data-mae`, `MRCs-AE-2024-plots-data-throughput`.