#!/bin/bash
shdir=$(pwd)
rm -rf /UnitxMakefile
mkdir UnixMakefile
cd UnixMakefile
cmake -G "Unix Makefiles" ./../src
cd $shdir
