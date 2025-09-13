# GridMR demo (Java master + C++ worker + shared filesystem)

- Master (Java, gRPC) assigns MAP/REDUCE tasks.
- Worker (C++, gRPC) runs demo `map`/`reduce` and writes intermediate/results.
- Data path uses a shared filesystem (NFS/EFS-style). In Docker we simulate it with a named volume mounted at `/shared`.

Quick start:
1. Build Java locally (optional): `mvn -DskipTests package`
2. Start stack: `docker compose -f infra/docker-compose.yml up --build`
3. Put input files into the `shared-data` volume. You can locate it via `docker volume inspect shared-data` and copy `testdata/input-001.txt` and `testdata/input-002.txt` there as `input-001.txt` and `input-002.txt`.
4. Watch logs of master and workers; MAP tasks will process inputs and reducers will write results under `/shared/results/<jobId>/`.

Config:
- `SHARED_DATA_ROOT` (default `/shared`)
- `MR_INPUT_URIS` (comma-separated list of relative paths under `SHARED_DATA_ROOT`, or absolute paths/http URLs)
- `MR_N_REDUCERS` number of reducers
- `MR_MAP_BIN_URI` and `MR_REDUCE_BIN_URI` may point to http URLs or local files; local files are resolved relative to `SHARED_DATA_ROOT` if not absolute.

Notes:
- Proto is in `src/main/proto/gridmr.proto` and shared by both sides.