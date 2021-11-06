#pragma once
#ifndef MYLSM_BLOOMFILTER_H
#define MYLSM_BLOOMFILTER_H
template<class K>
class bloomFilter{
    typedef unsigned long long ull;
    bloomFilter(ull capacity, double fp);
};

#endif