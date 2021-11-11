#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include <map>
#include <vector>
#include <set>
#include <functional>
#include <sstream>
#include <cassert>
#include <cstdint>
#include <cstddef>
#include <iostream>


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

int main()
{
    char script_infile[64] = "parser_in.txt"; //NULL;
    char script_outfile[64] = "parser_out.txt"; //NULL;
    FILE *script_input;
    FILE *script_output;

    if (script_infile) {
        script_input = fopen(script_infile, "r");
        if (script_input == NULL) {
            perror("Couldn't open input file");
        exit(1);
        }
    }
    if (script_outfile) {
        script_output = fopen(script_outfile, "w");
        if (script_output == NULL) {
            perror("Couldn't open output file");
        exit(1);
        }
    }

    int op;
    uint64_t target, ed;
    char new_val[10];

    while (1) {
        int r = demo_parser(script_input, &op, &target, &ed, new_val);
        if (r == EOF)
            exit(0);
        if (r < 0)
            exit(4);
        switch (op) 
        {
        case 0:
            fprintf(script_output, "inserting %ld with val %s\n", target, new_val);
            // do insert
            break;
        case 1:
            fprintf(script_output, "updating %ld with val %s\n", target, new_val);
            // do update
            break;
        case 2:
            fprintf(script_output, "Deleting %ld\n", target);
            // do insert
            break;
        case 3:
            // do query
            fprintf(script_output, "Query %ld\n", target);
            break;
        case 4:
            // do range query
            fprintf(script_output, "Range_query from %ld to %ld\n", target, ed);
            break;
        default:
            fprintf(script_output, "err~ \n");
            break;
        }
    }

}