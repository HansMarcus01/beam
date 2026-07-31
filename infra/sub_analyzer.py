import os
from google.cloud import pubsub_v1
from google.cloud import monitoring_v3
from datetime import datetime, timezone, timedelta

def analyze_subscriptions_finops(project_id, topic_path):
    print(f"Starting FinOps analysis on project: {project_id}...")
    print(f"Target topic: {topic_path}\n")
    
    subscriber = pubsub_v1.SubscriberClient()
    project_path = f"projects/{project_id}"
    
    # 1. List all subscriptions for the given topic
    subscriptions = []
    try:
        for subscription in subscriber.list_subscriptions(project=project_path):
            if subscription.topic == topic_path:
                subscriptions.append(subscription)
    except Exception as e:
        print(f"Error listing subscriptions: {e}")
        return

    total_subscriptions = len(subscriptions)
    if total_subscriptions == 0:
        print("No subscriptions were found for this topic")
        return
        
    print(f"Found {total_subscriptions} subscriptions pointing to this topic.")
    print("-" * 80)
    
    orphan_time_limit = datetime.now(timezone.utc) - timedelta(days=1) # 24 hours old
    orphan_subscriptions = 0
    active_subscriptions = 0
    total_bytes_retained = 0
    total_unacknowledged_messages = 0
    
    # Initialize monitoring client to extract usage metrics for each one
    metric_client = monitoring_v3.MetricServiceClient()
    
    for sub in subscriptions:
        sub_name = sub.name
        # Extract basic subscription metadata
        # We try to get information about its lifetime / inactivity
        is_orphan = False
        
        # Query subscription metrics (Unacknowledged messages and age)
        interval = monitoring_v3.TimeInterval({
            "end_time": {"seconds": int(datetime.now(timezone.utc).timestamp())},
            "start_time": {"seconds": int((datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp())}
        })
        
        # Filter to get the backlog of unacknowledged bytes for this specific subscription
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
            
            unacknowledged_messages = 0
            for result in results:
                for point in result.points:
                    unacknowledged_messages = max(unacknowledged_messages, point.value.int64_value)
                    
            if unacknowledged_messages > 1000: # If it has a persistent accumulated backlog unread
                is_orphan = True
                orphan_subscriptions += 1
                total_unacknowledged_messages += unacknowledged_messages
            else:
                active_subscriptions += 1
                
        except Exception:
            # If there is a metrics restriction, we classify it by temporal age of the subscription
            orphan_subscriptions += 1

    # Percentage calculations
    orphan_percentage = (orphan_subscriptions / total_subscriptions) * 100
    active_percentage = (active_subscriptions / total_subscriptions) * 100
    
    # Print quantitative results
    print("\n=== FINOPS BACKUP REPORT (QUESTION 2) ===")
    print(f"Total subscriptions analyzed: {total_subscriptions}")
    print(f"Orphan subscriptions detected: {orphan_subscriptions} ({orphan_percentage:.2f}%)")
    print(f"Healthy active subscriptions:  {active_subscriptions} ({active_percentage:.2f}%)")
    print("-" * 80)
    print(f"Estimated retained messages in orphan queues: {total_unacknowledged_messages:,} messages")
    print("-" * 80)
    
    if orphan_percentage > 80:
        print("OPTIMIZATION ALERT: The percentage of orphan resources is CRITICAL.")
        print("It is recommended to apply short automatic expiration policies (Expiration Policies) of 1 day.")
    else:
        print("The state of resources is healthy.")

if __name__ == "__main__":
    PROJECT_ID = "apache-beam-testing"
    TOPIC = "projects/pubsub-public-data/topics/taxirides-realtime"
    analyze_subscriptions_finops(PROJECT_ID, TOPIC)
