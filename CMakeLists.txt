cmake_minimum_required(VERSION 3.15)
project(MyLSM)

set(CMAKE_CXX_STANDARD 11)

add_executable(MyLSM main.cpp skipList.cpp skipList.h run.h KVpair.h diskRun.cpp diskRun.h)