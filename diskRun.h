//
// Created by Egoist on 2021/11/4.
//
#pragma once
#ifndef MYLSM_DISKRUN_H
#define MYLSM_DISKRUN_H
#include "run.h"
#include "bloom_filter.hpp"
#include <cstdlib>
#include <limits.h>
#include <string>
#include <vector>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
using namespace std;
typedef unsigned long long ull;
template<class K, class V>
class diskRun{
public:
    typedef KVpair<K, V> KVpair_t;
    KVpair_t *cache;
    int fd;
    unsigned int pageSize;
    bloom_filter bf;

    K _min_key = INT_MAX;
    K _max_key = INT_MIN;
    diskRun<K, V>(ull capacity, unsigned int pageSize, int level, int runID, double bf_fp);
    ~diskRun<K, V>();
    /*Write data from run to cache[offset] with length of len*/
    void write_data(const KVpair_t *run, const size_t offset, const ull& len);
    /*Construct fence pointer*/
    void construct_index();
    ull bs(const ull offset, const ull n, const K &key, bool &found);
    void get_flanking_fp(const K &key, ull &start, ull &end, bool &found);
    ull get_index(const K &key, bool &found);
    V   get(const K &key, bool &found);
    void get_from_range(const K &key1, const K &key2, ull &i1, ull &i2);
    ull size(){return _capacity;}
    void set_size(ull size){_capacity = size;}
private:
    ull _capacity;
    string _filename;
    int _level;
    vector<K> _fence_pointers;
    unsigned int _max_fp;
    unsigned int _runID;
    double _bf_fp;
    void do_unmap();
};
#endif //MYLSM_DISKRUN_H
