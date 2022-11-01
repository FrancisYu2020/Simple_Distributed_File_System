#!/bin/bash

for val in {1..9}
do
    echo VM$val Updating
    ssh tian23@fa22-cs425-220$val.cs.illinois.edu "rm -rf mp3-hangy6-tian23; git clone -b dev git@gitlab.engr.illinois.edu:hangy6/mp3-hangy6-tian23.git"
    echo VM$val Updated
done
echo VM10 Updating
ssh tian23@fa22-cs425-2210.cs.illinois.edu "rm -rf mp2-hangy6-tian23; git clone -b dev git@gitlab.engr.illinois.edu:hangy6/mp3-hangy6-tian23.git"
echo VM10 Updated

echo "-----------All VMs Have Been Updated!------------"