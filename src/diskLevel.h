#pragma once
#ifndef MYLSM_DISKLEVEL_H
#define MYLSM_DISKLEVEL_H
#include "diskRun.hpp"
#include "skipList.hpp"
#define LEFTCHILD(x) 2*x + 1
#define RIGHTCHILD(x) 2*x + 2
#define PARENT(x) (x - 1) /2

constexpr uint64_t TOMBSTONE = -1;

using namespace std;

template<class K, class V>
class diskLevel{
public:
    typedef KVpair<K, V> KVpair_t;
    typedef pair<KVpair<K, V>, int> KVIntpair_t;
    KVpair_t KVMAX;
    KVIntpair_t KVINTMAX;
    V V_TOMBSTONE = (V) TOMBSTONE;

    struct Heap{
        int size;
        vector<KVIntpair_t> arr;
        KVIntpair_t max;

        Heap(int sz, KVIntpair_t mx):arr(sz,mx),max(mx){
            size = 0;
        }
        void push(KVIntpair_t blob){
            int i = size++;
            while(i && blob < arr[PARENT(i)]){
                arr[i] = arr[PARENT(i)];
                i = PARENT(i);
            }
            arr[i] = blob;
        }

        void heapify(int i){
            int min = (LEFTCHILD(i) < size && arr[LEFTCHILD(i)] < arr[i]) ? LEFTCHILD(i) : i;
            if(RIGHTCHILD(i) < size && arr[RIGHTCHILD(i)] < arr[min]){
                min = RIGHTCHILD(i);
            }
            if(min != i){
                swap(arr[i], arr[min]);
                heapify(min);
            }
        }

        KVIntpair_t pop(){
            KVIntpair_t res = arr[0];
            arr[0] = arr[--size];
            heapify(0);
            return res;
        }
    };

    int _level;

    unsigned _pageSize;
    ull _run_size;
    unsigned _run_num;
    unsigned _cur_run;
    unsigned _merge_size;
    double _bf_fp;
    vector<diskRun<K, V>*> runs;

    diskLevel(unsigned pageSize, int level, ull runSize, unsigned runNum, unsigned mergeSize, double bf_fp);
    

    ~diskLevel();
    void add_runs(vector<diskRun<K, V> *> &runlist, bool last);
    void add_arrays(KVpair_t *runToAdd, const ull len);
    vector<diskRun<K, V> *> get_runs();

    void free_merged_runs(vector<diskRun<K, V>*> &toFree);
    bool full(){return _cur_run == _run_num;}
    bool empty(){return _cur_run == 0;}

    V get(const K &key, bool &found);
    ull element_size();
};
#endif