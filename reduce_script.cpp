#include <iostream>
#include <fstream>
#include <string>
using namespace std;

int main(int arcv, char** argv) {
	string word, curWord;
	int count, curCount;
	cin >> word >> count;
	while (cin >> curWord) {
		cin >> curCount;
		if (curWord == word) {
			count += curCount;
		}
		else {
			cout << word << '\t' << count << '\n';
			word = curWord;
			count = curCount;
		}
	}
	cout << word << '\t' << count << '\n';
	return 0;
}