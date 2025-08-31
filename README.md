# GridMR demo (Java master + C++ worker + MinIO)

- Master (Java, gRPC) assigns a demo MAP task.
- Worker (C++, gRPC) has hardcoded map fn (placeholder) and reports completion.
- Data path uses MinIO (S3-compatible). Replace MR_INPUT_S3_URI to point to your object.

Quick start:
1. Build Java locally (optional): `mvn -DskipTests package`
2. Start stack: `docker compose -f infra/docker-compose.yml up --build`
3. Open MinIO console at http://localhost:9001 (user/pass: minioadmin) to upload `input.txt` to bucket `gridmr`.
4. Watch logs of master and worker; the worker should receive a MAP task and complete.

Notes:
- C++ worker currently logs a placeholder instead of reading from MinIO; integrate AWS SDK S3 calls next.
- Proto is in `src/main/proto/gridmr.proto` and shared by both sides.