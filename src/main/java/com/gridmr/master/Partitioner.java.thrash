package com.gridmr.master;

import java.util.*;

/**
 * Partitioner utilidades para:
 * 1. Repartir una lista discreta de URIs entre N workers (round‑robin balanceado).
 * 2. Dividir ficheros grandes en splits (byte ranges) de tamaño objetivo.
 * 3. Agrupar esos splits en asignaciones equilibradas por volumen (greedy bin packing ligero).
 *
 * No accede directamente a S3: asume que el caller ya obtuvo tamaños (p.ej. via HEAD).
 */
public class Partitioner {

    /** Información de un fichero de entrada (URI + tamaño en bytes). */
    public static final class FileInfo {
        private final String uri;
        private final long sizeBytes;
        public FileInfo(String uri, long sizeBytes) {
            if (sizeBytes < 0) throw new IllegalArgumentException("sizeBytes < 0 for uri=" + uri);
            this.uri = uri;
            this.sizeBytes = sizeBytes;
        }
        public String uri() { return uri; }
        public long sizeBytes() { return sizeBytes; }
    }

    /** Split de un fichero (byte‑range). */
    public static final class FileSplit {
        private final String uri;
        private final long start;
        private final long length;
        public FileSplit(String uri, long start, long length) {
            if (start < 0 || length <= 0) throw new IllegalArgumentException("Invalid range for " + uri);
            this.uri = uri;
            this.start = start;
            this.length = length;
        }
        public String uri() { return uri; }
        public long start() { return start; }
        public long length() { return length; }
        public long end() { return start + length; }
    }

    /** Resultado de agrupar splits para un worker. */
    public static final class PartitionAssignment {
        private final List<FileSplit> splits = new ArrayList<>();
        private long totalBytes = 0;
        public void add(FileSplit s) { splits.add(s); totalBytes += s.length(); }
        public List<FileSplit> splits() { return splits; }
        public long totalBytes() { return totalBytes; }
    }

    /**
     * Método simple existente: reparte URIs discretas sin tamaños, casi uniforme.
     */
    public static List<List<String>> partition(List<String> inputUris, int numPartitions) {
        if (numPartitions <= 0) throw new IllegalArgumentException("numPartitions <= 0");
        List<List<String>> partitions = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) partitions.add(new ArrayList<>());
        for (int i = 0; i < inputUris.size(); i++) partitions.get(i % numPartitions).add(inputUris.get(i));
        return partitions;
    }

    /**
     * Divide cada fichero en splits de tamaño objetivo (último split puede ser menor).
     * @param files lista de ficheros con tamaño
     * @param targetSplitBytes tamaño objetivo (ej. 64MB). Min 1MB para evitar demasiados splits.
     */
    public static List<FileSplit> splitFiles(List<FileInfo> files, long targetSplitBytes) {
        long min = 1L << 20; // 1MB
        if (targetSplitBytes < min) targetSplitBytes = min;
        List<FileSplit> out = new ArrayList<>();
        for (FileInfo f : files) {
            long remaining = f.sizeBytes();
            long offset = 0;
            if (remaining == 0) continue; // ignorar vacíos
            while (remaining > 0) {
                long len = Math.min(targetSplitBytes, remaining);
                out.add(new FileSplit(f.uri(), offset, len));
                remaining -= len;
                offset += len;
            }
        }
        return out;
    }

    /**
     * Agrupa splits por carga (bytes) usando una cola prioritaria (menor carga primero) => balance.
     * @param splits lista de splits ya generados
     * @param numPartitions número de grupos deseado
     * @return lista de asignaciones ordenada (más libre primero al final del proceso no está garantizado)
     */
    public static List<PartitionAssignment> balanceSplits(List<FileSplit> splits, int numPartitions) {
        if (numPartitions <= 0) throw new IllegalArgumentException("numPartitions <= 0");
        // Inicializar min‑heap por totalBytes
        PriorityQueue<PartitionAssignment> heap = new PriorityQueue<>(Comparator.comparingLong(PartitionAssignment::totalBytes));
        for (int i = 0; i < numPartitions; i++) heap.add(new PartitionAssignment());
        // Heurística: ordenar splits de mayor a menor para mejor equilibrio (LPT)
        splits.stream()
                .sorted(Comparator.comparingLong(FileSplit::length).reversed())
                .forEach(s -> {
                    PartitionAssignment pa = heap.poll();
                    pa.add(s);
                    heap.add(pa);
                });
        return new ArrayList<>(heap);
    }

    /**
     * Pipeline completo: (1) splitFiles -> (2) balanceSplits. Devuelve asignaciones para workers.
     */
    public static List<PartitionAssignment> partitionBySize(List<FileInfo> files, long targetSplitBytes, int numPartitions) {
        List<FileSplit> splits = splitFiles(files, targetSplitBytes);
        return balanceSplits(splits, numPartitions);
    }
}