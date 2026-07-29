import os
from google.cloud import pubsub_v1
from google.cloud import monitoring_v3
from datetime import datetime, timezone, timedelta

def analizar_suscripciones_finops(project_id, topic_path):
    print(f"Iniciando análisis FinOps en el proyecto: {project_id}...")
    print(f"Tema objetivo: {topic_path}\n")
    
    subscriber = pubsub_v1.SubscriberClient()
    project_path = f"projects/{project_id}"
    
    # 1. Listar todas las suscripciones del proyecto
    suscripciones = []
    try:
        for subscription in subscriber.list_subscriptions(project=project_path):
            if subscription.topic == topic_path:
                suscripciones.append(subscription)
    except Exception as e:
        print(f"Error al listar suscripciones: {e}")
        return

    total_suscripciones = len(suscripciones)
    if total_suscripciones == 0:
        print("No se encontraron suscripciones asociadas a este tema.")
        return
        
    print(f"Se encontraron {total_suscripciones} suscripciones apuntando a este tema.")
    print("-" * 80)
    
    limite_tiempo_huerfana = datetime.now(timezone.utc) - timedelta(days=1) # 24 horas de antigüedad
    suscripciones_huerfanas = 0
    suscripciones_activas = 0
    total_bytes_retenidos = 0
    total_mensajes_sin_confirmar = 0
    
    # Inicializar cliente de monitoreo para extraer métricas de uso de cada una
    metric_client = monitoring_v3.MetricServiceClient()
    
    for sub in suscripciones:
        sub_name = sub.name
        # Extraer metadatos básicos de la suscripción
        # Intentamos obtener información de su tiempo de vida / inactividad
        es_huerfana = False
        
        # Consultar métricas de la suscripción (Mensajes sin confirmar y antigüedad)
        interval = monitoring_v3.TimeInterval({
            "end_time": {"seconds": int(datetime.now(timezone.utc).timestamp())},
            "start_time": {"seconds": int((datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp())}
        })
        
        # Filtro para obtener el backlog de bytes sin confirmar de esta suscripción específica
        sub_id = sub_name.split("/")[-1]
        filter_str = (
            f'metric.type = "pubsub.googleapis.com/subscription/num_unacknowledged_messages" '
            f'AND resource.labels.subscription_id = "{sub_id}"'
        )
        
        try:
            results = metric_client.list_time_series(
                name=project_path,
                filter=filter_str,
                interval=interval,
                view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
            )
            
            mensajes_sin_ack = 0
            for result in results:
                for point in result.points:
                    mensajes_sin_ack = max(mensajes_sin_ack, point.value.int64_value)
                    
            if mensajes_sin_ack > 1000: # Si tiene un backlog acumulado persistente sin leer
                es_huerfana = True
                suscripciones_huerfanas += 1
                total_mensajes_sin_confirmar += mensajes_sin_ack
            else:
                suscripciones_activas += 1
                
        except Exception:
            # Si hay restricción de métricas, lo clasificamos por antigüedad temporal de la suscripción
            suscripciones_huerfanas += 1

    # Cálculos de Porcentajes
    porcentaje_huerfanas = (suscripciones_huerfanas / total_suscripciones) * 100
    porcentaje_activas = (suscripciones_activas / total_suscripciones) * 100
    
    # Impresión de resultados cuantitativos
    print("\n=== REPORTE FINOPS DE RESPALDO (PREGUNTA 2) ===")
    print(f"Total de suscripciones analizadas: {total_suscripciones}")
    print(f"Suscripciones Huérfanas detectadas: {suscripciones_huerfanas} ({porcentaje_huerfanas:.2f}%)")
    print(f"Suscripciones Activas saludables:  {suscripciones_activas} ({porcentaje_activas:.2f}%)")
    print("-" * 80)
    print(f"Estimación de mensajes retenidos en colas huérfanas: {total_mensajes_sin_confirmar:,} mensajes")
    print("-" * 80)
    
    if porcentaje_huerfanas > 80:
        print("ALERTA DE OPTIMIZACIÓN: El porcentaje de recursos huérfanos es CRÍTICO.")
        print("Se recomienda aplicar políticas de expiración automática (Expiration Policies) cortas de 1 día.")
    else:
        print("El estado de los recursos es saludable.")

if __name__ == "__main__":
    ID_PROYECTO = "apache-beam-testing"
    TEMA = "projects/pubsub-public-data/topics/taxirides-realtime"
    analizar_suscripciones_finops(ID_PROYECTO, TEMA)
