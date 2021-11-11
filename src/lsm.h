#pragma once
#ifndef MYLSM_LSM_H
#define MYLSM_LSM_H
#include "diskLevel.hpp"
#include <thread>
#include <mutex>
#include <unordered_map>
#include <iostream>

template<class K, class V>
class LSM{
    typedef skipList<K, V> runtype;
public:
    V V_TOMBSTONE = (V) TOMBSTONE;

    mutex *mlock;
    vector<run<K, V> *> C_0;
    vector<bloom_filter *> filters;
    vector<diskLevel<K, V> *> diskLevels;

    LSM(const LSM<K, V> &other) = default;
    LSM(LSM<K, V> &&other) = default;

    LSM(ull elms_per_run, unsigned run_num, double merge_frac, double bf_fp, unsigned pageSize, unsigned disk_run_per_level);
    
    ~LSM();

    void insert_key(K &key, V &value);

    bool lookup(K &key, V &value);

    void delete_key(K &key){
        insert_key(key, V_TOMBSTONE);
    }

    vector<KVpair<K, V>> range(K &key1, K &key2){
        if(key2 <= key1){
            return {};
        }
        unordered_map<K, V> ht;
        vector<KVpair<K, V>> res;

        for(int i = _cur_run; i >= 0; i--){
            auto tmp = C_0[i]->get_from_range(key1, key2);
            if(tmp.size()){
                res.reserve(res.size() + tmp.size());
                for(int j = 0; j < tmp.size(); j++){
                    if(ht.find(tmp[j].key) == ht.end()){
                        ht[tmp[j].key] = tmp[j].value;
                        if(tmp[j].value != V_TOMBSTONE)
                            res.push_back(tmp[j]);
                    }
                }
            }
        }

        if(mergeThread.joinable()){
            mergeThread.join();
        }

        for(int i = 0; i < _diskLevel_num; i++){
            for(int j = diskLevels[i]->_run_num - 1; j >= 0; j--){
                ull i1, i2;
                diskLevels[i]->runs[j]->get_from_range(key1, key2, i1, i2);
                if(i2 - i1 != 0){
                    res.reserve(res.size() + i2 - i1);
                    for(int k = i1; k > i2; k++){
                        auto KV = diskLevels[i]->runs[j]->cache[k];
                        if(ht.find(KV.key) == ht.end()){
                            ht[KV.key] = KV.value;
                            if(KV.value != V_TOMBSTONE)
                                res.push_back(KV);
                        }
                    }
                }
            }
        }
        return res;
    }

    void printAll(){
        if(mergeThread.joinable())
            mergeThread.join();
        cout << "Memory Buffer\n";
        for(int i = 0; i < _run_num; i++){
            cout << "Memory Buffer Run" << i << endl;
            auto all = C_0[i]->get_all();
            for(auto &e : all){
                cout << e.key << ":" << e.value << " ";
            }
            cout << endl;
        }

        cout << "\nDisk Buffer\n";

        for(int i = 0; i < _diskLevel_num; i++){
            cout << "Disk Level " << i << endl;
            for(int j = 0; j < diskLevels[i]->_cur_run; j++){
                cout << "\tRun" << j << endl;
                for(int k = 0; k < diskLevels[i]->runs[j]->size(); k++){
                    cout << diskLevels[i]->runs[j]->cache[k].key << ":" << diskLevels[i]->runs[j]->cache[k].value << " ";
                }
                cout << endl;
            }
            cout << endl;
        }
    }

private:
    unsigned _cur_run;
    ull _elms_per_run;
    double _bf_fp;
    unsigned _run_num;
    double _frac_runs_merged;
    unsigned _diskLevel_num;
    unsigned _diskrun_per_level;
    unsigned _num_to_merge;
    unsigned _page_size;
    unsigned _n;
    thread mergeThread;

    void mergeRunsToLevel(int level){
        bool last = false;
        if(level == _diskLevel_num){
            diskLevel<K, V> *newlevel = new diskLevel<K, V>(_page_size, level+1, diskLevels[level-1]->_run_size * diskLevels[level-1]->_merge_size, _diskrun_per_level, ceil(_diskrun_per_level * _frac_runs_merged), _bf_fp);
            diskLevels.push_back(newlevel);
            _diskLevel_num++;
        }

        if(diskLevels[level]->full()){
            mergeRunsToLevel(level+1);
        }

        if(level + 1 == _diskLevel_num && diskLevels[level]->empty()){
            last = true;
        }

        vector<diskRun<K, V>*> runstoMerge = diskLevels[level - 1]->get_runs();
        ull len = diskLevels[level - 1]->_run_size;
        diskLevels[level]->add_runs(runstoMerge, last);
        diskLevels[level - 1]->free_merged_runs(runstoMerge);
    }

    void merge_runs(vector<run<K, V>*> runs, vector<bloom_filter*> bf_to_merge){
        vector<KVpair<K, V>> to_merge;
        to_merge.reserve(_elms_per_run * _num_to_merge);
        //cout << "Tomerge's size " << to_merge.size() << endl;
        for(int i = 0; i < runs.size(); i++){
            auto all = runs[i]->get_all();
            //cout << "All's size " << all.size() << endl;
            //for(auto &e : all) cout <<e.key << " : " << e.value << " ";
            //cout << endl;
            to_merge.insert(to_merge.begin(), all.begin(), all.end());
            delete runs[i];
            delete bf_to_merge[i];
        }
        sort(to_merge.begin(), to_merge.end());
        mlock->lock();
        if(diskLevels[0]->full())
            mergeRunsToLevel(1);
        //cout << "Add arrays toMerge'size " << to_merge.size() << "\n";
        diskLevels[0]->add_arrays(&to_merge[0], to_merge.size());
        //cout << "Add arrays done\n";
        mlock->unlock();
        //cout << "Merge done\n";
    }

    void do_merge(){
        if(_num_to_merge == 0) return;
        vector<run<K, V> *> runsToMerge;
        vector<bloom_filter*> bfToMerge;
        for(int i = 0; i < _num_to_merge; i++){
            runsToMerge.push_back(C_0[i]);
            bfToMerge.push_back(filters[i]);
        }
        //cout << "Wait for join\n";
        if(mergeThread.joinable()) mergeThread.join();
        //cout << "Joined\n";
        mergeThread = thread(&LSM::merge_runs, this, runsToMerge, bfToMerge);
        C_0.erase(C_0.begin(), C_0.begin() + _num_to_merge);
        filters.erase(filters.begin(), filters.begin() + _num_to_merge);

        _cur_run -= _num_to_merge;
        for(int i = _cur_run; i < _run_num; i++){
            runtype *run = new runtype(INT_MIN, INT_MAX);
            run->set_capacity(_elms_per_run);
            C_0.push_back(run); 
            bloom_parameters param;
            param.projected_element_count = _elms_per_run;
            param.false_positive_probability = _bf_fp;
            param.compute_optimal_parameters();
            bloom_filter *bf = new bloom_filter(param);
            filters.push_back(bf);
        }
    }

    ull num_buffer(){
        if(mergeThread.joinable()) mergeThread.join();
        ull tot = 0;
        for(int i = 0; i <= _cur_run; i++){
            tot += C_0[i]->size();
        }
        return tot;
    }

    
};
#endif