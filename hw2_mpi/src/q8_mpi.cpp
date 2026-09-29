#include <iostream>
#include <vector>
#include <mpi.h>
#include <fstream>
#include <iomanip>
#include <algorithm>
#include <unordered_map>
#include <cmath>
#include <limits>

using namespace std;

#pragma pack(push, 1)
struct Record {
    int ts, st;
    double t, h, p, r, w;
};
#pragma pack(pop)

struct StationStat {
    long long count;
    double temp_sum;
    double rain_sum;
};

// Custom comparison for Hottest/Coldest
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
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int N = 0, K = 0, S = 0;
    vector<Record> global_records;

    if (rank == 0) {
        if (argc > 1) {
            ifstream fin(argv[1]);
            fin >> N >> K >> S;
            global_records.resize(N);
            for (int i = 0; i < N; ++i) {
                fin >> global_records[i].ts >> global_records[i].st 
                    >> global_records[i].t >> global_records[i].h 
                    >> global_records[i].p >> global_records[i].r 
                    >> global_records[i].w;
            }
        } else {
            cin >> N >> K >> S;
            global_records.resize(N);
            for (int i = 0; i < N; ++i) {
                cin >> global_records[i].ts >> global_records[i].st 
                    >> global_records[i].t >> global_records[i].h 
                    >> global_records[i].p >> global_records[i].r 
                    >> global_records[i].w;
            }
        }
    }

    MPI_Bcast(&N, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&K, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&S, 1, MPI_INT, 0, MPI_COMM_WORLD);

    int base = N / size;
    int rem = N % size;
    int local_n = base + (rank < rem ? 1 : 0);

    vector<int> sendcounts(size), displs(size);
    int offset = 0;
    for (int i = 0; i < size; ++i) {
        int r = base + (i < rem ? 1 : 0);
        sendcounts[i] = r * sizeof(Record);
        displs[i] = offset * sizeof(Record);
        offset += r;
    }

    vector<Record> local_records(local_n);
    MPI_Scatterv(rank == 0 ? global_records.data() : nullptr, sendcounts.data(), displs.data(), 
                 MPI_BYTE, local_records.data(), local_n * sizeof(Record), MPI_BYTE, 0, MPI_COMM_WORLD);

    // Local computation variables
    double l_t_sum = 0, l_h_sum = 0, l_p_sum = 0, l_w_sum = 0, l_r_sum = 0;
    double l_t_max = -1e9, l_h_max = -1e9, l_p_max = -1e9, l_w_max = -1e9, l_r_max = -1e9;
    double l_t_min = 1e9,  l_h_min = 1e9,  l_p_min = 1e9;
    long long l_extreme_temp = 0;
    
    Record l_hottest = {0, 0, -1e9, 0, 0, 0, 0};
    Record l_coldest = {0, 0, 1e9, 0, 0, 0, 0};
    
    vector<long long> l_st_count(S, 0);
    vector<double> l_st_tsum(S, 0.0), l_st_rsum(S, 0.0);
    unordered_map<int, int> l_interval_counts;

    for (int i = 0; i < local_n; ++i) {
        Record& r = local_records[i];
        l_t_sum += r.t; l_h_sum += r.h; l_p_sum += r.p; l_w_sum += r.w; l_r_sum += r.r;
        
        l_t_max = max(l_t_max, r.t); l_h_max = max(l_h_max, r.h); 
        l_p_max = max(l_p_max, r.p); l_w_max = max(l_w_max, r.w); l_r_max = max(l_r_max, r.r);
        l_t_min = min(l_t_min, r.t); l_h_min = min(l_h_min, r.h); l_p_min = min(l_p_min, r.p);
        
        if (r.t >= 40.0 || r.t <= 0.0) l_extreme_temp++;
        if (compareHottest(r, l_hottest)) l_hottest = r;
        if (compareColdest(r, l_coldest)) l_coldest = r;
        
        l_st_count[r.st]++;
        l_st_tsum[r.st] += r.t;
        l_st_rsum[r.st] += r.r;
        
        l_interval_counts[r.ts / 60]++;
    }

    // Default bounds fix if local_n == 0
    if (local_n == 0) {
        l_t_max = l_h_max = l_p_max = l_w_max = l_r_max = -1e9;
        l_t_min = l_h_min = l_p_min = 1e9;
    }

    // Global reductions
    double g_t_sum, g_h_sum, g_p_sum, g_w_sum, g_r_sum;
    double g_t_max, g_h_max, g_p_max, g_w_max, g_r_max;
    double g_t_min, g_h_min, g_p_min;
    long long g_extreme_temp;

    MPI_Reduce(&l_t_sum, &g_t_sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&l_h_sum, &g_h_sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&l_p_sum, &g_p_sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&l_w_sum, &g_w_sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&l_r_sum, &g_r_sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    
    MPI_Reduce(&l_t_max, &g_t_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&l_h_max, &g_h_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&l_p_max, &g_p_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&l_w_max, &g_w_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&l_r_max, &g_r_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    MPI_Reduce(&l_t_min, &g_t_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&l_h_min, &g_h_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&l_p_min, &g_p_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    
    MPI_Reduce(&l_extreme_temp, &g_extreme_temp, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    vector<Record> gathered_hottest(size), gathered_coldest(size);
    MPI_Gather(&l_hottest, sizeof(Record), MPI_BYTE, gathered_hottest.data(), sizeof(Record), MPI_BYTE, 0, MPI_COMM_WORLD);
    MPI_Gather(&l_coldest, sizeof(Record), MPI_BYTE, gathered_coldest.data(), sizeof(Record), MPI_BYTE, 0, MPI_COMM_WORLD);

    vector<long long> g_st_count(S);
    vector<double> g_st_tsum(S), g_st_rsum(S);
    MPI_Reduce(l_st_count.data(), g_st_count.data(), S, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(l_st_tsum.data(), g_st_tsum.data(), S, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(l_st_rsum.data(), g_st_rsum.data(), S, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    // Interval counts Gather
    vector<int> l_int_keys, l_int_vals;
    for (auto& p : l_interval_counts) { l_int_keys.push_back(p.first); l_int_vals.push_back(p.second); }
    int l_int_size = l_int_keys.size();
    vector<int> g_int_sizes(size), int_displs(size, 0);
    MPI_Gather(&l_int_size, 1, MPI_INT, g_int_sizes.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);
    
    int total_ints = 0;
    if (rank == 0) {
        for (int i = 0; i < size; ++i) {
            int_displs[i] = total_ints;
            total_ints += g_int_sizes[i];
        }
    }
    vector<int> g_int_keys(total_ints), g_int_vals(total_ints);
    MPI_Gatherv(l_int_keys.data(), l_int_size, MPI_INT, g_int_keys.data(), g_int_sizes.data(), int_displs.data(), MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Gatherv(l_int_vals.data(), l_int_size, MPI_INT, g_int_vals.data(), g_int_sizes.data(), int_displs.data(), MPI_INT, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        Record final_hottest = gathered_hottest[0], final_coldest = gathered_coldest[0];
        for (int i = 1; i < size; ++i) {
            if (gathered_hottest[i].ts != 0 || gathered_hottest[i].t > -1e8) {
                if (compareHottest(gathered_hottest[i], final_hottest)) final_hottest = gathered_hottest[i];
            }
            if (gathered_coldest[i].ts != 0 || gathered_coldest[i].t < 1e8) {
                if (compareColdest(gathered_coldest[i], final_coldest)) final_coldest = gathered_coldest[i];
            }
        }

        unordered_map<int, int> g_intervals;
        for (int i = 0; i < total_ints; ++i) g_intervals[g_int_keys[i]] += g_int_vals[i];
        
        int best_interval = -1, max_interval_count = -1;
        for (auto& p : g_intervals) {
            if (p.second > max_interval_count || (p.second == max_interval_count && p.first < best_interval)) {
                max_interval_count = p.second;
                best_interval = p.first;
            }
        }

        vector<pair<long long, int>> st_sort;
        for (int i = 0; i < S; ++i) {
            if (g_st_count[i] > 0) st_sort.push_back({g_st_count[i], i});
        }
        sort(st_sort.begin(), st_sort.end(), [](const pair<long long, int>& a, const pair<long long, int>& b){
            if (a.first != b.first) return a.first > b.first;
            return a.second < b.second;
        });

        cout << fixed << setprecision(6);
        cout << "TOTAL_MEASUREMENTS " << N << "\n";
        cout << "AVERAGE_TEMPERATURE " << g_t_sum / N << "\n";
        cout << "MIN_TEMPERATURE " << g_t_min << "\n";
        cout << "MAX_TEMPERATURE " << g_t_max << "\n";
        cout << "AVERAGE_HUMIDITY " << g_h_sum / N << "\n";
        cout << "MIN_HUMIDITY " << g_h_min << "\n";
        cout << "MAX_HUMIDITY " << g_h_max << "\n";
        cout << "AVERAGE_PRESSURE " << g_p_sum / N << "\n";
        cout << "MIN_PRESSURE " << g_p_min << "\n";
        cout << "MAX_PRESSURE " << g_p_max << "\n";
        cout << "TOTAL_RAINFALL " << g_r_sum << "\n";
        cout << "MAX_RAINFALL " << g_r_max << "\n";
        cout << "AVERAGE_WIND_SPEED " << g_w_sum / N << "\n";
        cout << "MAX_WIND_SPEED " << g_w_max << "\n";
        cout << "EXTREME_TEMPERATURE_EVENTS " << g_extreme_temp << "\n";
        cout << "HOTTEST_MEASUREMENT " << final_hottest.ts << " " << final_hottest.st << " " << final_hottest.t << "\n";
        cout << "COLDEST_MEASUREMENT " << final_coldest.ts << " " << final_coldest.st << " " << final_coldest.t << "\n";
        cout << "BUSIEST_INTERVAL " << best_interval << " " << max_interval_count << "\n";
        cout << "TOP_STATIONS\n";
        for (int i = 0; i < min(K, (int)st_sort.size()); ++i) {
            int st = st_sort[i].second;
            cout << st << " " << g_st_count[st] << " " << g_st_tsum[st] / g_st_count[st] << " " << g_st_rsum[st] << "\n";
        }
    }

    MPI_Finalize();
    return 0;
}