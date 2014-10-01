@echo off
set batchdir="%~dp0"
if exist "Visual Studio 2012" rd "Visual Studio 2012" /s/q 
mkdir "Visual Studio 2012"
cd "Visual Studio 2012"
cmake -G "Visual Studio 11" ..\src
cd %batchdir%