#include <iostream>
#include <fstream>
#include <string>
#include <iomanip>
#include <stdlib.h>
using namespace std;

int main(int arcv, char** argv) {
	if (arcv < 2) {
		cerr << "Incorrect input\n";
		return 1;
	}
	int n = atoi(argv[1]);
	//int n = 10;
	int m = 0, n1 = n;
	while (n1 > 0) {
		n1 /= 10;
		m++;
	}
	double a;
	double step = double(1) / n;
	cin >> a;
	if (a == 1) {
		a -= step;
	}
	int snumb_of_steps = a / step;
	cout << setprecision(m) << double(step*snumb_of_steps) << '-' << double(step*(snumb_of_steps + 1)) << '\t' << 1 << endl;
	return 0;
}