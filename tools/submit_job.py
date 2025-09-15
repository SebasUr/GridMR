#!/usr/bin/env python3
import json
import os
import sys
import urllib.request

def main():
    host = os.environ.get('GRIDMR_MASTER_HOST', '18.212.222.54')
    port = int(os.environ.get('GRIDMR_MASTER_HTTP_PORT', '8080'))
    url = f'http://{host}:{port}/submit-job'

    # Payload mirroring previous compose defaults and seeder-provided files
    payload = {
        "input_uris": "input/input-001.txt,input/input-002.txt",
        "n_reducers": int(os.environ.get("GRIDMR_N_REDUCERS", "4")),
        # Use the C++ sources copied by the seeder (workers will compile them)
        "map_bin_uri": os.environ.get("GRIDMR_MAP_BIN_URI", "/map.cc"),
        "reduce_bin_uri": os.environ.get("GRIDMR_REDUCE_BIN_URI", "reduce.cc"),
        "group_partitioning": False,
        "min_workers": int(os.environ.get("GRIDMR_MIN_WORKERS", "4")),
        "start_delay_ms": int(os.environ.get("GRIDMR_START_DELAY_MS", "5000")),
        "desired_maps": int(os.environ.get("GRIDMR_DESIRED_MAPS", "2")),
    }

    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode('utf-8')
            print(body)
    except Exception as e:
        print(f"Failed to submit job: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
