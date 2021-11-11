#include "diskLevel.h"
#include <string>

template<class K, class V>
diskLevel<K, V>::diskLevel(unsigned int pageSize, int level, ull runSize, unsigned int runNum, unsigned int mergeSize, double bf_fp){
    _level = level;
    _pageSize = pageSize;
    _run_num = runNum;
    _run_size = runSize;
    _merge_size = mergeSize;
    _bf_fp = bf_fp;
    _cur_run = 0;
    KVMAX = {INT_MAX, 0};
    KVINTMAX = KVIntpair_t(KVMAX, -1);

    for(int i = 0; i < _run_num; i++){
        diskRun<K, V> *run = new diskRun<K, V>(_run_size, _pageSize, _level, i, _bf_fp);
        runs.push_back(run);
    }
}

template<class K, class V>
diskLevel<K, V>::~diskLevel(){
    for(int i = 0; i < this->runs.size(); i++){
        delete runs[i];
    }
}

template<class K, class V>
void diskLevel<K, V>::add_runs(vector<diskRun<K,V> *> &runlist, bool last){
    Heap pq = Heap(runlist.size(), KVINTMAX);
    vector<int> multi_ways(runlist.size(), 0);
    for(int i = 0; i < runlist.size(); i++){
        KVpair_t kvp = runlist[i]->cache[0];
        pq.push({kvp, i});
    }

    int j = -1;
    K lastKey = INT_MAX;
    unsigned lastRun = INT_MIN;
    while(pq.size != 0){
        auto top = pq.pop();
        if(lastKey == top.first.key){
            if(lastRun < top.second)
                runs[_cur_run]->cache[j] = top.first;
        }else{
            j++;
            if(j != -1 && last && runs[_cur_run]->cache[j].value == V_TOMBSTONE){
                j--;
            }
            runs[_cur_run]->cache[j] = top.first;
        }

        lastKey = top.first.key;
        lastRun = top.second;

        unsigned k = lastRun;
        if(++multi_ways[k] < runlist[k]->size()){
            pq.push(KVIntpair_t(runlist[k]->cache[multi_ways[k]], k));
        }
    }

    if(last && runs[_cur_run]->cache[j].value == V_TOMBSTONE){
        j--;
    }
    runs[_cur_run]->set_size(j+1);
    runs[_cur_run]->construct_index();
    if(j + 1 > 0) _cur_run++;
}

template<class K, class V>
void diskLevel<K, V>::add_arrays(KVpair_t *runToAdd, const ull len){
    runs[_cur_run]->write_data(runToAdd, 0, len);
    // cout << "Write done\n";
    runs[_cur_run]->construct_index();
    _cur_run++;
}


template<class K, class V>
vector<diskRun<K, V>*> diskLevel<K, V>::get_runs(){
    vector<diskRun<K,V>*> toMerge;
    for(int i = 0; i < _merge_size; i++)
        toMerge.push_back(runs[i]);
    return toMerge;
}

template<class K, class V>
void diskLevel<K, V>::free_merged_runs(vector<diskRun<K,V> *> &toFree){
    if(toFree.size() != _merge_size){
        perror("Disk Level free size doesn't match");
    }
    for(int i = 0; i < _merge_size; i++){
        delete toFree[i];
    }

    runs.erase(runs.begin(), runs.begin() + _merge_size);
    _cur_run -= _merge_size;
    for(int i = 0; i < _cur_run; i++){
        runs[i]->_runID = i;
        string newname = ("C_" + to_string(runs[i]->_level) + "_" + to_string(runs[i]->_runID) + ".txt");

        if(rename(runs[i]->_filename.c_str(), newname.c_str())){
            perror(("Error renaming file " + runs[i]->_filename + " to " + newname).c_str());
            exit(-1);
        }
        runs[i]->_filename = newname;
    }

    for(int i = _cur_run; i < _run_num; i++){
        diskRun<K, V> *newrun = new diskRun<K, V>(_run_size, _pageSize, _level, i, _bf_fp);
        runs.push_back(newrun);
    }
}

template<class K, class V>
V diskLevel<K, V>::get(const K &key, bool &found){
    int maxRunToSearch = full()? _run_num -1 : _cur_run -1;
    for(int i = maxRunToSearch; i >= 0; i--){
        V value = runs[i]->get(key, found);
        if(found){
            return value;
        }
    }
    return (V) NULL;
}

template<class K, class V>
ull diskLevel<K, V>::element_size(){
    ull res = 0;
    for(int i = 0; i < _cur_run; i++){
        res += runs[i]->get_capacity();
    }
    return res;
}

