#include <iostream>
#include "lsm.hpp"
#include <random>
#include <algorithm>
#include <utility>
#include <cassert>
#include <string>
#include <sys/time.h>
#include <sys/types.h>


struct LSMParams{
    const int num_insert;
    const int num_runs;
    const int elms_per_run;
    const double bf_fp;
    const int page_size;
    const int diskrun_per_level;
    const double merge_frac;
};


void timer_start(uint64_t &timer)
{
  struct timeval t;
  //assert(!gettimeofday(&t, NULL));
  gettimeofday(&t, NULL);
  timer -= 1000000*t.tv_sec + t.tv_usec;
  //cout << t.tv_usec << endl;
}

void timer_stop(uint64_t &timer)
{
  struct timeval t;
//  assert(!gettimeofday(&t, NULL));
  gettimeofday(&t, NULL);
  timer += 1000000*t.tv_sec + t.tv_usec;
}


void benchmark_upserts(LSM<uint64_t, uint64_t> &lsm,
			   uint64_t nops,
			   uint64_t number_of_distinct_keys,
			   uint64_t random_seed)
{
  uint64_t overall_timer = 0;
  for (uint64_t j = 0; j < 100; j++) {
	uint64_t timer = 0;
	timer_start(timer);
	for (uint64_t i = 0; i < nops / 100; i++) {
	  uint64_t t = rand() % number_of_distinct_keys;
      auto value = t * 100 + rand() % 10;
	  lsm.insert_key(t, value);
	}
	timer_stop(timer);
	printf("%lld %lld %lld\n", j, nops/100, timer);
	overall_timer += timer;
  }
  printf("# overall: %lld %lld\n", 100*(nops/100), overall_timer);
}

void benchmark_queries(LSM<uint64_t, uint64_t> &b,
			   uint64_t nops,
			   uint64_t number_of_distinct_keys,
			   uint64_t random_seed)
{
  
  // Pre-load the tree with data
  srand(random_seed);
  for (uint64_t i = 0; i < nops; i++) {
	  uint64_t t = rand() % number_of_distinct_keys;
    auto value = i * 100 + rand() % 10;
	  b.insert_key(i, value);
  }

	// Now go back and query it
  srand(random_seed);
  uint64_t overall_timer = 0;
	timer_start(overall_timer);
  for (uint64_t i = 0; i < nops; i++) {
	uint64_t t = rand() % number_of_distinct_keys;
    uint64_t value;
    auto found = b.lookup(i, value);
    if(found)
      cout << i << " : " << value << endl;
  }
  b.printAll();
	timer_stop(overall_timer);
  printf("# overall: %lld %lld\n", nops, overall_timer);

}


int demo_parser(FILE *input, int *op, 
                uint64_t *arg, uint64_t *arg2, 
                char *new_val) 
{
    int ret;
    char command[64];
    //char new_val[64];

    ret = fscanf(input, "%s %ld", command, arg);
    if (ret == EOF)
        return EOF;
    else if (ret != 2) {
        fprintf(stderr, "Parse error\n");
        exit(3);
    }

    if (strcmp(command, "Insert") == 0) {
        *op = 0;
        if (1 != fscanf(input, " %s", new_val)) {
            fprintf(stderr, "Parse error\n");
            exit(3);
        }
    } else if (strcmp(command, "Update") == 0) {
        *op = 1;
        if (1 != fscanf(input, " %s", new_val)) {
            fprintf(stderr, "Parse error\n");
            exit(3);
        }
    } else if (strcmp(command, "Delete") == 0) {
        *op = 2;
    } else if (strcmp(command, "Query") == 0) {
        *op = 3;
    } else if (strcmp(command, "Range_query") == 0){ //3 
        *op = 4;
        if (1 != fscanf(input, " %ld", arg2)){
            fprintf(stderr, "Parse error\n");
            exit(3);
        }
    } else {
        fprintf(stderr, "Unknown command: %s\n", command);
        exit(1);
    }
    
    return 0;
}

int main(int argc, char *argv[]){
    auto lsm =  LSM<uint64_t, uint64_t>(800,20,1.0,0.00100,1024,20);
    cout << "Created \n";
    unsigned randseed = time(NULL) * getpid();
    //benchmark_upserts(lsm,1ul<<20,1ul<<20, randseed);
    benchmark_queries(lsm,1ul<<20,1ul<<14, randseed);

    // char script_infile[64] = "parser_in.txt"; //NULL;
    // char script_outfile[64] = "parser_out.txt"; //NULL;
    // FILE *script_input;
    // FILE *script_output;

    // if (script_infile) {
    //     script_input = fopen(script_infile, "r");
    //     if (script_input == NULL) {
    //         perror("Couldn't open input file");
    //     exit(1);
    //     }
    // }
    // if (script_outfile) {
    //     script_output = fopen(script_outfile, "w");
    //     if (script_output == NULL) {
    //         perror("Couldn't open output file");
    //     exit(1);
    //     }
    // }

    // int op;
    // uint64_t target, ed;
    // char new_val[10];
    // auto lsm  = LSM<uint64_t, string>(800,20,1.0,0.00100,1024,20);
    // while (1) {
    //     int r = demo_parser(script_input, &op, &target, &ed, new_val);
    //     if (r == EOF)
    //         exit(0);
    //     if (r < 0)
    //         exit(4);
    //     switch (op) 
    //     {
    //     case 0:{
    //         fprintf(script_output, "inserting %ld with val %s\n", target, new_val);
    //         // do insert
    //         auto str = string(new_val);
    //         lsm.insert_key(target, str);
    //         break;
    //     }
    //     case 1:
    //     {
    //         fprintf(script_output, "updating %ld with val %s\n", target, new_val);
    //         // do update
    //         auto str = string(new_val);
    //         lsm.insert_key(target, str);
    //         break;
    //     }
    //     case 2:
    //     {
    //         fprintf(script_output, "Deleting %ld\n", target);
    //         // do insert
    //         lsm.delete_key(target);
    //         break;
    //     }
    //     case 3:
    //     {    // do query
    //         fprintf(script_output, "Query %ld\n", target);
    //         string ret;
    //         bool found = lsm.lookup(target, ret);
    //         if(found){
    //             //do somethin
    //         }
    //         break;
    //     }
    //     case 4:
    //     {    // do range query
    //         fprintf(script_output, "Range_query from %ld to %ld\n", target, ed);
    //         auto ret = lsm.range(target, ed);
    //         break;
    //     }
    //     default:
    //         fprintf(script_output, "err~ \n");
    //         break;
    //     }
    // }
}