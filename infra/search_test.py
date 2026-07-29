import os
from google.cloud import dataflow_v1beta3

def buscar_clase_origen_dataflow(project_id):
    client = dataflow_v1beta3.JobsV1Beta3Client()
    request = dataflow_v1beta3.ListJobsRequest(
        project_id=project_id,
        view=dataflow_v1beta3.JobView.JOB_VIEW_ALL  # Vista completa para traer los parámetros detallados
    )
    
    print(f"Inspeccionando origen de pruebas en: {project_id}...\n")
    print(f"{'Trabajo Crítico':<45} | {'Clase de Prueba de Origen / Archivo'}")
    print("-" * 100)
    
    for job in client.list_jobs(request=request):
        nombre = job.name
        # Enfocarnos en los trabajos críticos detectados de más de 10 min
        if "read-pubsub" in nombre or "write-pubsub" in nombre:
            origen = "No especificado"
            # Buscar en los parámetros del entorno de Dataflow la clase de prueba
            options = job.environment.sdk_pipeline_options.get("options", {}) if job.environment else {}
            
            # Buscar indicadores comunes del archivo de origen
            if "jobName" in options:
                origen = options.get("jobName")
            if "original_options" in options:
                origen = str(options.get("original_options"))
            
            # Revisar las propiedades del sistema si es una prueba de Java
            user_agent = job.environment.userAgent if job.environment else {}
            if "name" in user_agent:
                origen = f"{origen} (SDK: {user_agent.get('name')})"
                
            print(f"{nombre:<45} | {origen}")

if __name__ == "__main__":
    buscar_clase_origen_dataflow("apache-beam-testing")
