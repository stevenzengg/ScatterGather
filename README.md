# ScatterGather Device Driver

This GitHub project was performed using GitHub Classroom and had commit and workflow history erased. Here are the features introduced throughout the project:
1. File System Operations: Read, Write, Seek, and Update
2. Clean Data Storage Usage when sending requests to OS
3. LRU Cache dynamically stored on disk
4. Enhanced Read, Write, and Seek Operations to Exact Byte Precision

## Getting Started

1. Ensure your computer is operating in a linux or unix OS before continuing.
2. In the ScatterGather directory, run ```$ make```. This will
3. Run ```./sg_sim.c cmpsc311-assign5-workload.txt```. This command runs the simulation built into sg_sim.c with the parameter of the cmpsc311-assign5-workload.txt file, which has a total of 5000 operations spread out through 50 files. The simulation output is then validated based upon a generated final result before the simulation shuts down the driver.
