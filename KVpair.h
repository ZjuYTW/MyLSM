//
// Created by Egoist on 2021/11/4.
//

#ifndef MYLSM_KVPAIR_H
#define MYLSM_KVPAIR_H

template <class K, class V>
class KVpair {
public:
    K key;
    V value;

    bool operator == (const KVpair& other) const{
        return other.key == key && other.value == value;
    }

    bool operator != (const KVpair& other) const{
        return other.key != key || other.value != value;
    }

    bool operator < (const KVpair& other) const{
        return key < other.key;
    }

    bool operator > (const KVpair& other) const{
        return key > other.key;
    }
};


#endif //MYLSM_KVPAIR_H
