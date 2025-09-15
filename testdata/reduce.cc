#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

// Simple reducer for wordcount: reads lines "word\tcount" and sums counts
int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);
    std::unordered_map<std::string, long long> agg;
    std::string line;
    while (std::getline(std::cin, line)) {
        auto tab = line.find('\t');
        if (tab == std::string::npos) continue;
        std::string key = line.substr(0, tab);
        long long val = 0;
        try { val = std::stoll(line.substr(tab+1)); } catch (...) { continue; }
        agg[key] += val;
    }
    for (const auto &kv : agg) {
        std::cout << kv.first << '\t' << kv.second << '\n';
    }
    return 0;
}