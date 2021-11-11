
#include <string.h>
#include <stdio.h>
#include <sys/types.h>

#include <fstream>
#include <vector>
#include <string>

#define NOP (1UL << 12)
#define DISTINCT_KEYS (1UL << 10)

#define INSERT_RATIO 0.3    
#define UPDATE_RATIO 0.3
#define DELETE_RATIO 0.2
#define QUERY_RATIO 0.1
#define RANGE_QUERY_RATIO 0.1

int main()
{  
    std::vector<double> prob = {INSERT_RATIO};
    for (auto r : {UPDATE_RATIO, DELETE_RATIO, QUERY_RATIO, RANGE_QUERY_RATIO})
        prob.push_back(r + prob.back());
    //std::vector<double> prob = {0.3,    0.6,    0.8,    0.9,    1.0};
                                // insert   update  delete  query   range_query

    uint64_t key, ed;
    double t;

    FILE* destFile = fopen("parser_in.txt", "w");

    for (auto i = 0; i < NOP; i++)
    {
        t = rand() / double(RAND_MAX);
        key = rand() % DISTINCT_KEYS;
        ed = rand() % DISTINCT_KEYS;
        if (key > ed)
            std::swap(key, ed);
        
        if (t <= prob[0]) {
            fprintf(destFile, "Insert %lu %lu_v%d\n", key, key, rand() % 10);
        } else if (t <= prob[1]){
            fprintf(destFile, "Update %lu %lu_v%d\n", key, key, rand() % 10);
        } else if (t <= prob[2]){
            fprintf(destFile, "Delete %lu\n", key);
        } else if (t <= prob[3]){
            fprintf(destFile, "Query %lu\n", key);
        } else if (t <= prob[4]){
            fprintf(destFile, "Range_query %lu %lu\n", key, ed);
        } 

    }
    if (destFile)
        fclose(destFile);
    printf("Generating rand values..Done!\n");
    return 0;
}
