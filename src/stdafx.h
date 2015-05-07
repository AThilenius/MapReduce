//
//  stdafx.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//
#define _CRT_SECURE_NO_DEPRECATE
#define _SCL_SECURE_NO_WARNINGS

#define SAFE_FREE(ptr) if (ptr != nullptr) {free(ptr); ptr = nullptr;}

#include <iostream>
#include <unordered_map>
#include <string>
#include <fstream>
#include <sstream>
#include <functional>
#include <ctime>
#include <streambuf>