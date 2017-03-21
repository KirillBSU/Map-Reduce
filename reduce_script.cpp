#include <iostream>
#include <fstream>
#include <string>
using namespace std;

int main(int arcv, char** argv) {
	string word;
	int count = 0, curCount;
	while (cin >> word) {
		cin >> curCount;
		count += curCount;
	}
	cout << word << '\t' << count << '\n';
	return 0;
}