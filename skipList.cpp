//
// Created by Egoist on 2021/11/4.
//

#include "skipList.h"
#include <random>
#include <string.h>
template <class K, class V, int MAXLEVEL>
skipList<K, V, MAXLEVEL>::skipList(const K min_key, const K max_key):_min_key(min_key), _max_key(max_key), _max_level(MAXLEVEL) {
    p_listHead = new Node(min_key);
    p_listTail = new Node(max_key);
    _min = (K) NULL;
    _max = (K) NULL;
    _cur_level = 1;
    _size = 0;
    for(int i = 1; i <= _max_level; i++){
        p_listHead->forward[i] = p_listTail;
    }
}

template <class K, class V, int MAXLEVEL>
skipList<K, V, MAXLEVEL>::~skipList() {
    //Just need to delete the lowest level's node
    Node *cur = p_listHead;
    while(cur != p_listTail){
        Node *next = cur->forward[1];
        delete cur;
        cur = next;
    }
}

template <class K, class V, int MAXLEVEL>
void skipList<K, V, MAXLEVEL>::insert(const K& key, const V& value) {
    //Set max and min, for further search
    if(!_max || key > _max){
        _max = key;
    }else if(!_min || key < _min){
        _min = key;
    }

    Node *update[MAXLEVEL+1];
    Node *cur = p_listHead;
    //Do search from up to bottom level
    for(int i = _cur_level; i >= 1; i--){
        while(cur->forward[i]->key < key){
            cur = cur->forward[i];
        }
        update[i] = cur;
    }
    //If the key is in the skipList, then we update it
    if(cur->forward[1]->key == key){
        cur->forward[1]->value = value;
    }else{
        //Insert a new Node
        int insert_hight = generate_node_level();
        if(insert_hight > _cur_level){
            _cur_level = insert_hight;
            for(int i = _cur_level + 1; i <= insert_hight; i++){
                update[i] = p_listHead;
            }
        }

        auto curNode = new Node(key, value);
        for(int i = 1; i <= _cur_level; i++){
            curNode->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = curNode;
        }
        ++_size;
    }
}

template <class K, class V, int MAXLEVEL>
void skipList<K, V, MAXLEVEL>::erase(const K& key){
    Node *update[MAXLEVEL + 1];
    Node *cur = p_listHead;
    for(int i = _cur_level; i >= 1; i--){
        while(cur->forward[i]->key > key){
            cur = cur->forward[i];
        }
        update[i] = cur;
    }
    if(cur->forward[1]->key == key){
        Node *to_delete = cur->forward[1];
        //Means we find the key
        for(int i = _cur_level; i >= 1; i--){
            if(update[i]->forward[i]->key == key){
                //Means in this level, we found the key.
                //Now try to delete it
                update[i]->forward[i] = update[i]->forward[i]->forward[i];
            }
        }
        delete to_delete;
        //Now update cur_level
        for(int i = _cur_level; i >= 1; i--){
            if(p_listHead->forward[i] == p_listTail){
                _cur_level = i-1;
            }
        }
        _size--;
    }
}

template <class K, class V, int MAXLEVEL>
V skipList<K, V, MAXLEVEL>::get(const K& key, bool &found) {
    if(key < _min || key > _max) return (V) NULL;
    Node *cur = p_listHead;
    for(int i = _cur_level; i >= 1; i--){
        while(cur->forward[i]->key < key){
            cur = cur->forward[i];
        }
    }
    if(cur->forward[1]->key == key){
        found = true;
        return cur->forward[1]->value;
    }
    return (V) NULL;
}

template <class K, class V, int MAXLEVEL>
vector<KVpair<K, V>> skipList<K, V, MAXLEVEL>::get_all() {
    vector<KVpair<K,V>> res(_size);
    Node *cur = p_listHead->forward[1];
    auto idx = 0;
    while(cur != p_listTail){
        res[idx] = {cur->key, cur->value};
    }
    return res;
}

template <class K, class V, int MAXLEVEL>
vector<KVpair<K, V>> skipList<K, V, MAXLEVEL>::get_from_range(const K &key1, const K &key2) {
    //Maybe firstly should assert key1 < key2
    if(key1 > _max || key2 < _min){
        return {};
    }
    Node *cur = p_listHead;
    vector<KVpair<K, V>> res;
    for(int i = _cur_level; i >= 1; i--){
        while(cur->forward[i]->key < key1){
            cur = cur->forward[i];
        }
    }
    while(cur->forward[1]->key <= key2){// No need to judge if the Tail
        res.push_back({cur->forward[1]->key, cur->forward[1]->value});
    }
    return res;
}

template <class K, class V, int MAXLEVEL>
int skipList<K, V, MAXLEVEL>::generate_node_level() {
    return ffs(rand() & ((1 << MAXLEVEL) -1))
}