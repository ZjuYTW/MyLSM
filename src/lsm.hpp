#include "lsm.h"

template<class K,class V>
LSM<K, V>::LSM(ull elms_per_run, unsigned run_num, double merge_frac, double bf_fp, unsigned pageSize, unsigned disk_run_per_level){
    _elms_per_run = elms_per_run;
    _run_num = run_num;
    _frac_runs_merged = merge_frac;
    _diskrun_per_level = disk_run_per_level;
    _num_to_merge = ceil(_frac_runs_merged * run_num);
    _page_size = pageSize;
    _cur_run = 0;
    _bf_fp = bf_fp;
    _n = 0;
    diskLevel<K, V> *diskl = new diskLevel<K, V>(_page_size, 1, _num_to_merge * _elms_per_run, _diskrun_per_level, ceil(_diskrun_per_level * _frac_runs_merged),_bf_fp);

    diskLevels.push_back(diskl);
    _diskLevel_num = 1;
    
    for(int i = 0; i < _run_num; i++){
        runtype *run = new runtype(INT_MIN, INT_MAX);
        run->set_capacity(_elms_per_run);
        C_0.push_back(run);

        bloom_parameters param;
        param.projected_element_count = elms_per_run;
        param.false_positive_probability = bf_fp;
        param.compute_optimal_parameters();
        bloom_filter *bf = new bloom_filter(param);
        filters.push_back(bf);
    }
    mlock = new mutex();
}

template<class K,class V>
LSM<K, V>::~LSM(){
    if(mergeThread.joinable()){
        mergeThread.join();
    }
    delete mlock;
    for(int i = 0; i < C_0.size(); i++){
        delete C_0[i];
        delete filters[i];
    }

    for(int i = 0; i < diskLevels.size(); i++){
        delete diskLevels[i];
    }
}

template<class K, class V>
void LSM<K, V>::insert_key(K &key, V &value){
    if(C_0[_cur_run]->size() >= _elms_per_run){
        _cur_run++;
    }
    if(_cur_run >= _run_num){
        //cout << "Need to merge" <<endl;
        do_merge();
        //cout << "Back" << endl;
    }
    C_0[_cur_run]->insert(key, value);
    //cout << "C0 insert " << key << " "<< value <<endl;
    filters[_cur_run]->insert(key);
}

template<class K, class V>
bool LSM<K, V>::lookup(K &key, V &value){
    bool found = false;
    for(int i = _cur_run; i >= 0; i--){
        value = C_0[i]->get(key, found);
        if(found)
            return value != V_TOMBSTONE;
    }
    if(mergeThread.joinable()){
        mergeThread.join();
    }
    for(int i = 0; i < _diskLevel_num; i++){
        value = diskLevels[i]->get(key, found);
        if(found){
            return value != V_TOMBSTONE;
        }
    }
    return false;
}

