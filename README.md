# Multi-Column Compaction (MCC)

This is the repository for Multi-Column Compaction in IoTDB. 

The evaluation code is based on IoTDB Python API.

User Guide for setting up the python environment for IoTDB Python API could be found here: https://iotdb.apache.org/UserGuide/V0.13.x/API/Programming-Python-Native-API.html

## File Structure

+ `src/`: include all the scripts, database and datasets used for evaluations

  + `src/iotdb-server`: the iotdb instance, implemented with our proposal and packaged
  + `src/iotdb`: iotdb python interface
  + `src/dataset`: samples of all the datasets used for experiments
  + `evaluation.py`: the entry of the method
  + `SessionClear.py`: remove all the data in IoTDB

  

## Steps

+ Install the python packages for the project according to `requirements.txt` and `requirements_dev.txt`
+ unzip `dataset.zip`
+ unzip the iotdb server 
+ In command line, change dir to the `iotdb-server/sbin` and type `./start-server.sh`  (or `sbin\start-server.bat` in Windows), instructions could be find in https://iotdb.apache.org/UserGuide/V0.13.x/QuickStart/QuickStart.html.
+ Run `evaluation.py` 
