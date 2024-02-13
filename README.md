# TTLsMatter-EuroSys24
 TTLs Matter: Efficient Cache Sizing with TTL-Aware Miss Ratio Curves and Working Set Sizes

Although the code is cross-platform and supports most operating systems including Linux, Windows, and MacOS, For the artifact evaluation, we assume Ubuntu 23.04 is used. 

## Dependencies

### 1/2 dotnet sdk v8.0.100 
Following are the commands you need to execute to download and install the sdk on Ubuntu 23.04. 
* wget https://download.visualstudio.microsoft.com/download/pr/5226a5fa-8c0b-474f-b79a-8984ad7c5beb/3113ccbf789c9fd29972835f0f334b7a/dotnet-sdk-8.0.100-linux-x64.tar.gz
* mkdir -p $HOME/dotnet && tar zxf dotnet-sdk-8.0.100-linux-x64.tar.gz -C $HOME/dotnet
* export DOTNET_ROOT=$HOME/dotnet  [note: better to add to .bashrc]
* export PATH=$PATH:$HOME/dotnet	[note: better to add to .bashrc]

### 2/2 Gnuplot
We have used v5.4 which is the default installed with 'apt install'
* sudo apt install gnuplot 

## Compilation 
1. go to "src" directory 
2. run "./publish.sh" 

This will create a compiled binary called "TTLsMatter" inside "src/bin" directory. 

## Workloads 
* The formatted workloads used in this study can be downloaded from (TODO: uploading). They require ~700GB in compressed format, and 2.7TB in uncompressed binary format (the binary format used is detailed in "src/Datasets/Twitter/FilteredTwitterTracesReader.cs"). 
-- After downloading the formatted workloads and decompressed them (make sure each file ends with '.bin', e.g., cluster4.bin), you need to configure the code to point to the directory that contains the traces which is in "src/Datasets/Common/DatasetConfig.cs" (variable name  *TwitterTracesDir*)

* _Alternatively_, the raw version of the Twitter workloads can be obtained from https://github.com/twitter/cache-trace
-- We downloaded the version from SNIA, which require approximately 18TB for the decompressed CSV files. We then formatted them into binary following the process detailed in "src/Datasets/Twitter/FilteredTwitterTracesReader.cs" (this is a time consuming process, takes approximately a week)

## Reproducing the evaluation section results 
The process of reproducing results often involves a complex series of steps: generating the initial results, processing these results into CSV files, and finally executing plotting scripts to produce the necessary figures.

We have dedicated considerable effort to streamline this process, ensuring that you can replicate the results from our evaluation section (specifically, Figures 14, 15, 16, 17A, 17B, and 18) with a single command.

To do so, simply execute “./src/bin/TTLsMatter”. Upon completion, you will find all the generated plots within the “src/bin/Figures” directory, ready for your review.


