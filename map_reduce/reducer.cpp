// reducer
#include <string>
#include <map>
#include <iostream>
#include <iterator>
using namespace std;
int 
main()
{
	string key;
	string value;
	map < string, int > word2count;
	while (cin >> key) {
		cin >> value;
		auto it = word2count.find(key);
		if (it != word2count.end()) {
			(it->second)++;
		} else {
			word2count.insert(make_pair(key, 1));
		}
	}

	for (auto it = word2count.begin(); it != word2count.end(); ++it) {
		cout << it->first << "\t" << it->second << endl;
	}
	return 0;
}
