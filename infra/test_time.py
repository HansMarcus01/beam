import os
from google.cloud import dataflow_v1beta3
from datetime import datetime

def obtener_tiempos_de_pruebas_dataflow(project_id):
    # Inicializa el cliente de Dataflow
    client = dataflow_v1beta3.JobsV1Beta3Client()
    
    # Solicita la lista de trabajos del proyecto
    request = dataflow_v1beta3.ListJobsRequest(
        project_id=project_id,
        view=dataflow_v1beta3.JobView.JOB_VIEW_SUMMARY
    )
    
    print(f"Buscando trabajos de Dataflow en el proyecto: {project_id}...\n")
    print(f"{'Nombre del Trabajo (Prueba)':<50} | {'Estado':<12} | {'Duración (Minutos)':<18}")
    print("-" * 88)
    
    paginas = client.list_jobs(request=request)
    
    for job in paginas:
        # Filtramos por nombres comunes de las pruebas de taxi de Beam
        nombre_lower = job.name.lower()
        if "taxi" in nombre_lower or "pubsub" in nombre_lower:
            # Extraer timestamps de creación y estado actual
            start_time = job.create_time
            
            # Si el trabajo ya terminó, calculamos el tiempo de ejecución
            if job.current_state in [
                dataflow_v1beta3.JobState.JOB_STATE_DONE,
                dataflow_v1beta3.JobState.JOB_STATE_FAILED,
                dataflow_v1beta3.JobState.JOB_STATE_CANCELLED
            ]:
                # Buscamos el tiempo de cambio de estado final
                end_time = job.current_state_time
                duracion = end_time - start_time
                duracion_minutos = round(duracion.total_seconds() / 60.0, 2)
            else:
                duracion_minutos = "En ejecución..."
                
            estado_legible = str(job.current_state).replace("JOB_STATE_", "")
            print(f"{job.name:<50} | {estado_legible:<12} | {duracion_minutos:<18}")

if __name__ == "__main__":
    # Cambia esto por tu ID de proyecto si es diferente
    ID_PROYECTO = "apache-beam-testing"
    obtener_tiempos_de_pruebas_dataflow(ID_PROYECTO)
