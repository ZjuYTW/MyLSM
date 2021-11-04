//
// Created by Egoist on 2021/11/4.
//
#pragma once
#ifndef MYLSM_SKIPLIST_H
#define MYLSM_SKIPLIST_H

#include "run.h"
using namespace std;
template <class K, class V, int MAXLEVEL>
class skipList_Node{
public:
    const K key;
    V value;
    skipList_Node<K,V,MAXLEVEL>* forward[MAXLEVEL + 1];
    skipList_Node(const K key):key(key){
        for(int i = 1; i <= MAXLEVEL; i++) forward[i] = NULL;
    }
    ~skipList_Node(){}
};

template <class K, class V, int MAXLEVEL = 12>
class skipList : public run<K,V>{
    typedef skipList_Node<K, V, MAXLEVEL> Node;
public:
    const int _max_level;
    K _min, _max;
    K _min_key, _max_key;
    unsigned long long _size;
    unsigned long long _capacity;
    int _cur_level;
    Node *p_listHead, *p_listTail;
    skipList(const K min_key, const K max_key);
    ~skipList();
    void insert(const K&, const V&);
    void erase(const K&);
    V get(const K&, bool &found);
    vector<KVpair<K,V>> get_all();
    vector<KVpair<K,V>> get_from_range(const K& key1, const K& key2);
    unsigned long long size(){
        return _size;
    }
    void set_capacity(unsigned long long size){
        _capacity = size;
    }

private:
    int generate_node_level();
};
#endif //MYLSM_SKIPLIST_H
