import os
import sys
import time
import json
import subprocess
import requests
from dotenv import load_dotenv

load_dotenv()

MASTER_SSH = os.getenv("GRIDMR_MASTER_SSH")
REMOTE_BASE = os.getenv("GRIDMR_REMOTE_BASE")
MASTER_API = os.getenv("GRIDMR_MASTER_API")
CHUNK_SIZE_MB = int(os.getenv("GRIDMR_CHUNK_SIZE_MB"))
PEM_FILE = os.getenv("GRIDMR_PEM_FILE")

def run(cmd):
    """Ejecuta un comando del sistema y muestra lo que hace"""
    print(">>", cmd)
    subprocess.check_call(cmd, shell=True)

def split_input_file(input_file, output_dir="input", chunk_size_mb=1):
    """Parte un input.txt en varios archivos de ~chunk_size_mb MB, respetando líneas"""
    os.makedirs(output_dir, exist_ok=True)
    chunk_size = chunk_size_mb * 1024 * 1024

    part_num = 1
    current_size = 0
    current_lines = []

    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            encoded = line.encode("utf-8")
            line_size = len(encoded)
            
            if current_size + line_size > chunk_size and current_lines:
                out_path = os.path.join(output_dir, f"input-{part_num:03}.txt")
                with open(out_path, "w", encoding="utf-8") as out:
                    out.writelines(current_lines)
                print(f"[split] Created {out_path}")
                part_num += 1
                current_size = 0
                current_lines = []

            current_lines.append(line)
            current_size += line_size
        
        if current_lines:
            out_path = os.path.join(output_dir, f"input-{part_num:03}.txt")
            with open(out_path, "w", encoding="utf-8") as out:
                out.writelines(current_lines)
            print(f"[split] Created {out_path}")

    return [f"{output_dir}/input-{i:03}.txt" for i in range(1, part_num + 1)]

def fetch_result(job_id, local_dir="results"):
    """Descarga el resultado del cluster al cliente local"""
    os.makedirs(local_dir, exist_ok=True)
    remote_result = f"{REMOTE_BASE}/results/{job_id}/final.txt"
    local_result = os.path.join(local_dir, f"result-{job_id}.txt")

    pem_opt = f'-i "{PEM_FILE}"'
    run(f"scp {pem_opt} {MASTER_SSH}:{remote_result} {local_result}")

    print(f"[downloaded] {local_result}")
    return local_result

def remote_file_exists(remote_path):
    """Verifica por SSH si existe un archivo en el master"""
    pem_opt = f'-i "{PEM_FILE}"'
    cmd = f"ssh {pem_opt} {MASTER_SSH} 'test -f {remote_path}'"
    print(">>", cmd)
    
    return subprocess.call(cmd, shell=True) == 0

def wait_for_result(job_id, timeout=600, poll_interval=5):
    """Espera hasta que final.txt exista en el master (o timeout)"""
    remote_result = f"{REMOTE_BASE}/results/{job_id}/final.txt"
    print(f"[wait] Esperando resultado en {remote_result} (timeout={timeout}s)...")
    start = time.time()
    while time.time() - start < timeout:
        if remote_file_exists(remote_result):
            print("[wait] Resultado disponible.")
            return remote_result
        time.sleep(poll_interval)
    raise TimeoutError(f"No se encontró el resultado en {remote_result} dentro del tiempo esperado.")

def main():
    if len(sys.argv) != 5:
        print("Uso: gridmr_submit.py <map_file> <reduce_file> <input_file> <n_reducers>")
        sys.exit(1)

    map_file, reduce_file, input_file, n_reducers = sys.argv[1:5]
    job_id = str(int(time.time()))
    remote_path = f"{REMOTE_BASE}/{job_id}"

    print(f"=== GridMR Submit ===")
    print(f"Job ID: {job_id}")
    print(f"Map file: {map_file}")
    print(f"Reduce file: {reduce_file}")
    print(f"Input file: {input_file}")
    print(f"Remote path: {remote_path}\n")
    print(f"Number of reducers: {n_reducers}")

    input_parts = split_input_file(input_file, "input", CHUNK_SIZE_MB)

    pem_opt = f'-i "{PEM_FILE}"'

    run(f"ssh {pem_opt} {MASTER_SSH} 'mkdir -p {remote_path}/input'")
    run(f"scp {pem_opt} {map_file} {reduce_file} {MASTER_SSH}:{remote_path}/")
    run(f"scp {pem_opt} {' '.join(input_parts)} {MASTER_SSH}:{remote_path}/input/")
    
    payload = {
        "job_id": job_id,
        "input_uris": ",".join([f"{job_id}/input/{os.path.basename(p)}" for p in input_parts]),
        "n_reducers": n_reducers,
        "map_bin_uri": os.path.basename(map_file),
        "reduce_bin_uri": os.path.basename(reduce_file)
    }

    print(f"[payload]\n{json.dumps(payload, indent=2)}\n")
    
    try:
        res = requests.post(MASTER_API, json=payload, timeout=10)
        print("[master response]", res.status_code, res.text)
        res_json = res.json()
        job_id = res_json.get("job_id")
        if not job_id:
            print("No se recibió job_id en la respuesta del master.", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"Failed to notify master: {e}", file=sys.stderr)
        sys.exit(1)
    
    try:
        wait_for_result(job_id, timeout=600, poll_interval=5)
    except Exception as e:
        print(f"[wait error] {e}", file=sys.stderr)
        sys.exit(1)

    fetch_result(job_id)

if __name__ == "__main__":
    main()
