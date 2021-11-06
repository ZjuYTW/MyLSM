//
// Created by Egoist on 2021/11/4.
//

#include "diskRun.h"
template<class K, class V>
diskRun<K, V>::diskRun(ull capacity, unsigned int pageSize, int level, int runID, double bf_fp):_capacity(capacity), pageSize(pageSize),_level(level),_runID(runID), _bf_fp(bf_fp),bf(capacity, bf_fp){
    _filename = "C_" + to_string(level) + "_" + to_string(runID) + ".txt";
    size_t fsize = capacity * sizeof(KVpair_t);
    
    fd = open(_filename, O_RDWRm | O_CREAT | O_TRUNC, (mode_t)0600);
    if(fd == -1){
        perror(("Error in open the file" + _filename).c_str());
        exit(1);
    }
    //set the last byte of 
    auto offset = lseek(fd, fsize - 1, SEEK_SET);
    if(offset == -1){
        close(fd);
        perror(("Error calling lseek() to 'stretch' the file" + _filename).c_str());
        exit(1);
    }
    cache = (KVpair_t*) mmap(0, fsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if(cache == MAP_FAILED){
        close(fd);
        perror(("Error calling mmap() to map the file" + _filename).c_str());
        exit(1);
    }

}

template<class K, class V>
diskRun<K, V>::~diskRun(){
    fsync(fd);
    do_unmap();

    if(remove(_filename.c_str())){
        perror(("Error calling remove() to clean the file" + _filename).c_str());
        exit(1);
    }
}

template<class K, class V>
void diskRun<K, V>::write_data(const KVpair_t *run, const size_t offset, const ull& len){
    memcpy(cache + offset, run , len * sizeof(KVpair_t));
    _capacity = len;
}

template<class K, class V>
void diskRun<K, V>::construct_index(){
    _fence_pointers.reserve(_capacity / pageSize);
    _max_fp = -1;
    for(int i = 0; i < _capacity; i++){
        bf.add((K*) &cache[i].key, sizeof(K));
        if(i % pageSize == 0){
            _fence_pointers.push_back(cache[i].key);
            _max_fp++;
        }
    }

    _min_key = map[0].key;
    _max_key = map[_capacity-1].key;
}

template<class K, class V>
ull diskRun<K, V>::bs(const ull offset, const ull n, const K &key, bool &found){
    if(n == 0){
        found = true;
        return offset;
    }
    ull l = offset, r = offset + n - 1;
    while(l < r){
        auto mid = l + (r - l) / 2;
        if(key > cache[mid].key)
            l = mid + 1;
        else
            r = mid;
    }
    if(cache[l].key == key){
        found = true;
    }
    return l;
}

template<class K, class V>
void diskRun<K, V>::get_flanking_fp(const K &key, ull &start, ull &end, bool &found){
    if(_max_fp == 0){
        // means just one fence, just capacity < pageSize
        start = 0;
        end = _capacity;
        found = true;
    }else if(key < _fence_pointers[0].key){
        found = false;
    }else{
        ull l = 0, r = _max_fp;
        while(l < r){
            auto mid = l + (r - l) / 2;
            if(_fence_pointers[mid] <= key)
                l = mid + 1;
            else
                r = mid;
        }
        found = true;
        start = (l - 1) * pageSize;
        end = l * pageSize;
    }
}


template<class K, class V>
ull diskRun<K, V>::get_index(const K &key, bool &found){
    ull start, end;
    get_flanking_fp(key, start, end, found);
    ull ret = 0;
    if(found) ret = bs(start, end - start, key, found);
    return ret;
}

template<class K, class V>
V diskRun<K, V>::get(const K &key, bool &found){
    ull retv = get_index(key, found);
    return found ?  cache[retv].value : (V) NULL;
}

template<class K, class V>
void diskRun<K, V>::get_from_range(const K &key1, const K &key2, ull &i1, ull&i2){
    i1 = i2 = 0;
    if(key2 < _min_key || key1 > _max_key)
        return;
    if(key1 >= _min_key){
        bool found = false;
        i1 = get_index(key1, found);
    }
    if(key2 >= _max_key){
        i2 = _capacity;
        return;
    }else{
        bool found = false;
        i2 = get_index(key2, found);
    }
}

template<class K, class V>
void diskRun<K, V>::

