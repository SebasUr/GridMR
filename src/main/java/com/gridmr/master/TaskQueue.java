package com.gridmr.master;

import com.gridmr.proto.AssignTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Optional;

/**
 * TaskQueue almacena las tareas pendientes por asignar a los workers.
 */
public class TaskQueue {
    private final ConcurrentLinkedQueue<AssignTask> queue = new ConcurrentLinkedQueue<>();

    // Agrega una tarea a la cola
    public void addTask(AssignTask task) {
        queue.add(task);
    }

    // Obtiene y elimina la siguiente tarea de la cola (o vacío si no hay)
    public Optional<AssignTask> pollTask() {
        return Optional.ofNullable(queue.poll());
    }

    // Consulta si hay tareas pendientes
    public boolean hasTasks() {
        return !queue.isEmpty();
    }

    // Puedes agregar métodos para consultar el tamaño, listar tareas, etc.
}