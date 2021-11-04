//
// Created by Egoist on 2021/11/4.
//
#pragma once
#ifndef MYLSM_RUN_H
#define MYLSM_RUN_H
#include "KVpair.h"
#include <vector>
template <class K, class V>
class run {
public:
    virtual V get(const K&, bool &found) = 0;
    virtual K get_min_key() = 0;
    virtual K get_max_key() = 0;
    virtual void insert(const K&, const V&) = 0;
    virtual void erase(const K&) = 0;
    virtual unsigned long long size() = 0;
    virtual void set_capacity(unsigned long long) = 0;
    virtual std::vector<KVpair<K, V>> get_all() = 0;
    virtual std::vector<KVpair<K, V>> get_from_range(const K&, const K&) = 0;
};


#endif //MYLSM_RUN_H
