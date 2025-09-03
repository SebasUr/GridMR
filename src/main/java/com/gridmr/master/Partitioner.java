package com.gridmr.master;

import java.util.List;
import java.util.ArrayList;

/**
 * Partitioner divide el input en N partes, donde N es la cantidad de workers activos.
 * El input puede ser una lista de URIs (por ejemplo, splits en S3).
 */
public class Partitioner {

    /**
     * Divide la lista de URIs en N partes lo más equilibradas posible.
     * @param inputUris Lista de URIs de entrada.
     * @param numPartitions Número de particiones (workers activos).
     * @return Lista de listas, cada una con las URIs asignadas a una partición.
     */
    public static List<List<String>> partition(List<String> inputUris, int numPartitions) {
        List<List<String>> partitions = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new ArrayList<>());
        }
        for (int i = 0; i < inputUris.size(); i++) {
            partitions.get(i % numPartitions).add(inputUris.get(i));
        }
        return partitions;
    }
}