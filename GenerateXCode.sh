shdir=$(pwd)
rm -rf /XCode
mkdir XCode
cd XCode
cmake -G "Xcode" ./../src
cd $shdir