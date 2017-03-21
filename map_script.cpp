#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <regex>
using namespace std;

int main(int arcv, char** argv) {
	regex word("[^[:alpha:]|' ']+");
	const string format = " ";
	string str; 
	getline(cin, str);
		str = regex_replace(str, word, format, regex_constants::format_default);
		transform(str.begin(), str.end(), str.begin(), ::tolower);
		stringstream stream;
		stream << str;

		string tmp; 
		while(stream >> tmp) {
			cout << tmp << '\t' << 1 << endl;
		}
	return 0;
}
	