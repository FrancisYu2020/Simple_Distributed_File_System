# MP3-hangy6-tian23 Simple Distributed File System

## Description
Simple Distributed File System (SDFS) is an HDFS-like flat file system, which is easy to scale as the number of services increases. It also supports failure tolerance, fast replication, version control, and consistency levels features. 


## Installation

You can clone this project to the machines you need to grep log from using following command:

```
ssh: git clone git@gitlab.engr.illinois.edu:hangy6/mp3-hangy6-tian23.git
```
```
https: git clone https://gitlab.engr.illinois.edu/hangy6/mp3-hangy6-tian23.git
``` 

## Setup
To use this SDFS, you need to setup required enviroment first. To make this easy, we provide a requirements.txt, please use the following command in simpleDFS/ 

```
pip3 install -r requirements.txt
```

## Usage
The default settings of introducer and Namenode can be configured in simpleDFS/server.py.
To use SDFS, first you need to start the Datanode and failure detector in the machines you want to provide store services. Please use the following command in simpleDFS/:

```
python3 server.py
```

And use following command to join the memberlist of failure detector, please notice that you need to first join the introducer before other nodes:
```
join
```

To list the current membership list, on any node run:
```
list_mem
```

Then you can use following command in simpleDFS/ to start interactive command shell to use the store file system:
```
python3 client.py
```

We support follwing commands:
```
put [localfilename] [sdfsfilename]
get [sdfsfilename] [localfilename]
delete [sdfsfilename]
ls [sdfsfilename]
store
get-versions [sdfsfilename] [numversions] [localfilename]
```

## Support
If you have any questions, please contact tian23@illinois.edu or hangy6@illinois.edu

## Authors 
Tian Luan & Hang Yu
