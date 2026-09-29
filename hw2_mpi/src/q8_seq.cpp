#include <iostream>
#include <vector>
#include <fstream>
#include <iomanip>
#include <algorithm>
#include <unordered_map>
#include <cmath>
using namespace std;

struct Record {
    int ts, st; double t, h, p, r, w;
};

bool compareHottest(const Record& a, const Record& b) {
    if (a.t != b.t) return a.t > b.t;
    if (a.ts != b.ts) return a.ts < b.ts;
    return a.st < b.st;
}
bool compareColdest(const Record& a, const Record& b) {
    if (a.t != b.t) return a.t < b.t;
    if (a.ts != b.ts) return a.ts < b.ts;
    return a.st < b.st;
}

int main(int argc, char** argv) {
    int N, K, S;
    if (argc > 1) {
        ifstream fin(argv[1]);
        fin >> N >> K >> S;
    } else { cin >> N >> K >> S; }
    
    ifstream fin(argc > 1 ? argv[1] : "/dev/stdin");
    if (argc > 1) { int tmp; fin >> tmp >> tmp >> tmp; } 

    double t_sum=0, h_sum=0, p_sum=0, w_sum=0, r_sum=0;
    double t_max=-1e9, h_max=-1e9, p_max=-1e9, w_max=-1e9, r_max=-1e9;
    double t_min=1e9, h_min=1e9, p_min=1e9;
    long long extreme_temp=0;
    
    Record hottest = {0, 0, -1e9, 0, 0, 0, 0}, coldest = {0, 0, 1e9, 0, 0, 0, 0};
    vector<long long> st_count(S, 0);
    vector<double> st_tsum(S, 0.0), st_rsum(S, 0.0);
    unordered_map<int, int> interval_counts;

    for (int i = 0; i < N; ++i) {
        Record r;
        fin >> r.ts >> r.st >> r.t >> r.h >> r.p >> r.r >> r.w;
        t_sum+=r.t; h_sum+=r.h; p_sum+=r.p; w_sum+=r.w; r_sum+=r.r;
        t_max=max(t_max, r.t); h_max=max(h_max, r.h); p_max=max(p_max, r.p); w_max=max(w_max, r.w); r_max=max(r_max, r.r);
        t_min=min(t_min, r.t); h_min=min(h_min, r.h); p_min=min(p_min, r.p);
        
        if (r.t >= 40.0 || r.t <= 0.0) extreme_temp++;
        if (compareHottest(r, hottest)) hottest = r;
        if (compareColdest(r, coldest)) coldest = r;
        
        st_count[r.st]++; st_tsum[r.st]+=r.t; st_rsum[r.st]+=r.r;
        interval_counts[r.ts / 60]++;
    }

    int best_interval = -1, max_interval_count = -1;
    for (auto& p : interval_counts) {
        if (p.second > max_interval_count || (p.second == max_interval_count && p.first < best_interval)) {
            max_interval_count = p.second; best_interval = p.first;
        }
    }

    vector<pair<long long, int>> st_sort;
    for (int i = 0; i < S; ++i) {
        if (st_count[i] > 0) st_sort.push_back({st_count[i], i});
    }
    sort(st_sort.begin(), st_sort.end(), [](const pair<long long, int>& a, const pair<long long, int>& b){
        if (a.first != b.first) return a.first > b.first;
        return a.second < b.second;
    });

    cout << fixed << setprecision(6);
    cout << "TOTAL_MEASUREMENTS " << N << "\n";
    cout << "AVERAGE_TEMPERATURE " << t_sum / N << "\n";
    cout << "MIN_TEMPERATURE " << t_min << "\n";
    cout << "MAX_TEMPERATURE " << t_max << "\n";
    cout << "AVERAGE_HUMIDITY " << h_sum / N << "\n";
    cout << "MIN_HUMIDITY " << h_min << "\n";
    cout << "MAX_HUMIDITY " << h_max << "\n";
    cout << "AVERAGE_PRESSURE " << p_sum / N << "\n";
    cout << "MIN_PRESSURE " << p_min << "\n";
    cout << "MAX_PRESSURE " << p_max << "\n";
    cout << "TOTAL_RAINFALL " << r_sum << "\n";
    cout << "MAX_RAINFALL " << r_max << "\n";
    cout << "AVERAGE_WIND_SPEED " << w_sum / N << "\n";
    cout << "MAX_WIND_SPEED " << w_max << "\n";
    cout << "EXTREME_TEMPERATURE_EVENTS " << extreme_temp << "\n";
    cout << "HOTTEST_MEASUREMENT " << hottest.ts << " " << hottest.st << " " << hottest.t << "\n";
    cout << "COLDEST_MEASUREMENT " << coldest.ts << " " << coldest.st << " " << coldest.t << "\n";
    cout << "BUSIEST_INTERVAL " << best_interval << " " << max_interval_count << "\n";
    cout << "TOP_STATIONS\n";
    for (int i = 0; i < min(K, (int)st_sort.size()); ++i) {
        int st = st_sort[i].second;
        cout << st << " " << st_count[st] << " " << st_tsum[st] / st_count[st] << " " << st_rsum[st] << "\n";
    }
    return 0;
}