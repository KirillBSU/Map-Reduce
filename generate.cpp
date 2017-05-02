#include <iostream>
#include <fstream>
#include <string>
#include <iomanip>
#include <stdlib.h>
#include <time.h>
using namespace std;

int main(int arcv, char** argv) {
	if (arcv < 3) {
		cerr << "Incorrect input\n";
		return 1;
	}
	int n = atoi(argv[1]);
	ofstream cout(argv[2]);

	srand(time(NULL));
	for (int i = 0; i < n; i++) {
		cout << '\t' << double(rand() % RAND_MAX) / double(RAND_MAX) << endl;
	}
	return 0;
}