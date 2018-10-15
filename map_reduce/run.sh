g++ -std=c++14 mapper.cpp -o mapper
g++ -std=c++14 reducer.cpp -o reducer
cat input | ./mapper | sort | ./reducer
