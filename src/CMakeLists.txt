cmake_minimum_required(VERSION 2.8)

# Project Layout
# - PROJECT_HOME_DIRECTORY
#   - bin
#   - src
#   - libs
#     - include
#     - lib

# Paths
STRING(REPLACE "src" "" PROJECT_HOME_DIRECTORY ${CMAKE_HOME_DIRECTORY})
string(REGEX REPLACE "(^[\\/]|[\\/]$)" "" PROJECT_HOME_DIR_TRIMMED "${PROJECT_HOME_DIRECTORY}")
string(REGEX MATCH "([^\\/]+)$" PROJECT_NAME "${PROJECT_HOME_DIR_TRIMMED}")
SET(SRC_DIRECTORY "${PROJECT_HOME_DIRECTORY}src")
SET(INCLUDE_DIRECTORY "${PROJECT_HOME_DIRECTORY}libs/include")
SET(LIBS_DIRECTORY "${PROJECT_HOME_DIRECTORY}libs/lib")

MESSAGE(" ")
MESSAGE(" #                 Project Name: ${PROJECT_NAME}")
MESSAGE(" #       Project Home Directory: ${PROJECT_HOME_DIRECTORY}")
MESSAGE(" #             Source Directory: ${SRC_DIRECTORY}")
MESSAGE(" # Additional Include Directory: ${INCLUDE_DIRECTORY}")
MESSAGE(" #    Additional Libs Directory: ${LIBS_DIRECTORY}")
MESSAGE(" ")

# Project Spec
project(${PROJECT_NAME})
SET(EXE_NAME ${PROJECT_NAME})

# Set Include / Lib folders
INCLUDE_DIRECTORIES(${SRC_DIRECTORY})
INCLUDE_DIRECTORIES(${INCLUDE_DIRECTORY})
LINK_DIRECTORIES(${LIBS_DIRECTORY})

# GLOB all files
FILE(GLOB_RECURSE SOURCE_FILES ${SRC_DIRECTORY}/*.h ${SRC_DIRECTORY}/*.cpp)
# FILE(GLOB_RECURSE testFiles ${PROJECT_HOME_DIRECTORY}/src/*.Test.cpp)
# list(REMOVE_ITEM SOURCE_FILES ${testFiles})

# Auto setup directory structure, and add each path as a
# include directory
foreach(FILE ${SOURCE_FILES}) 
  get_filename_component(PARENT_DIR "${FILE}" PATH)

  # Add it as an include directory
  INCLUDE_DIRECTORIES(${PARENT_DIR})

  # Traim trailing slashes in both
  string(REGEX REPLACE "(^\\|\\$)" "" PARENT_DIR_TRIMMED "${PARENT_DIR}")
  string(REGEX REPLACE "(^\\|\\$)" "" SRC_DIRECTORY_TRIMMED "${SRC_DIRECTORY}")

  # Make relative
  STRING(REPLACE "${SRC_DIRECTORY_TRIMMED}" "" GROUP ${PARENT_DIR_TRIMMED})
  string(REPLACE "/" "\\" GROUP "${GROUP}")
  source_group("${GROUP}" FILES "${FILE}")
endforeach()

# Add external libs as relative paths
FILE(GLOB thirdPartyLibrariesRaw ${LIBS_DIRECTORY}/*.lib)
FOREACH(library ${thirdPartyLibrariesRaw})
 STRING(REPLACE "${LIBS_DIRECTORY}/" "" shortLibrary ${library})
 SET(thirdPartyLibraries ${thirdPartyLibraries} -l:${shortLibrary})
ENDFOREACH()

# C++ 11
ADD_DEFINITIONS(-std=c++11)

# Set EXE and Linker options
SET(LINK_OPTIONS " ")
ADD_EXECUTABLE(${EXE_NAME} ${SOURCE_FILES} )
SET_TARGET_PROPERTIES(${EXE_NAME} PROPERTIES LINK_FLAGS ${LINK_OPTIONS})
TARGET_LINK_LIBRARIES(${EXE_NAME} ${thirdPartyLibraries})