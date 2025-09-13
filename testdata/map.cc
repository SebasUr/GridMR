#include <iostream>
#include <sstream>
#include <string>
#include <cctype>

// Simple wordcount mapper: reads stdin, emits "word\t1" per token
int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);
    std::string line;
    while (std::getline(std::cin, line)) {
        // normalize non-alnum to space
        for (char &c : line) {
            if (!std::isalnum(static_cast<unsigned char>(c))) c = ' ';
            else c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        std::istringstream iss(line);
        std::string word;
        while (iss >> word) {
            std::cout << word << '\t' << 1 << '\n';
        }
    }
    return 0;
}
