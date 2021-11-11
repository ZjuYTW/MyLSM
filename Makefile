all:
	g++ -g -w -fpermissive -std=c++11 -O3 src/main.cpp -o output -lpthread

clean:
	rm output
