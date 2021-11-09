#include <iostream>
#include "lsm.hpp"

int main(int argc, char *argv[]){

//    insertLookupTest();
//    updateDeleteTest();
//    rangeTest();
//    rangeTimeTest();
//    concurrentLookupTest();
//    tailLatencyTest();
//    cartesianTest();
//    updateLookupSkewTest();
    
    auto lsm = LSM<int, int>(800,20,1.0,0.00100,1024,20);
    auto strings = vector<string>(3);
    if (argc == 2){
    cout << "LSM Tree DSL Interactive Mode" << endl;
        while (true){
            cout << "> ";
            string input;
            getline(cin, input);
            queryLine(lsm, input, strings);
        }
    }
    else{
        string line;
        ifstream f;
        for (int i = 1; i < argc; ++i){
            f.open(argv[i]);
            
            if(!f.is_open()) {
                perror("Error open");
                exit(EXIT_FAILURE);
            }
            while(getline(f, line)) {
                queryLine(lsm, line, strings);
            }
        }
    }







    
    
    
    return 0;
}