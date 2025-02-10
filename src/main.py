import sys
import threading
from time import sleep

from src.scraper.kafka_producer import HespressDataCollector
from src.scraper.wait_for_kafka import wait_for_kafka
from src.dashboard.dashboard import DashboardApp
from flask_socketio import SocketIO

def run_collector(collector):
    while True:
        collector.collect_comments()
        sleep(20)  # Run every 20 seconds

def run_dashboard():
    socketio = SocketIO(DashboardApp().app)
    socketio.run(DashboardApp().app, host='0.0.0.0', port=5000, debug=False)

if __name__ == "__main__":
    wait_for_kafka(max_retries=15, retry_delay=15)
    
    collector = HespressDataCollector()
    
    # Start collector in a separate thread
    collector_thread = threading.Thread(target=run_collector, args=(collector,))
    collector_thread.daemon = True  # Make thread daemon so it exits when main thread exits
    collector_thread.start()
    
    # Start dashboard in another thread
    dashboard_thread = threading.Thread(target=run_dashboard)
    dashboard_thread.daemon = True  # Make thread daemon so it exits when main thread exits
    dashboard_thread.start()
    
    try:
        # Keep the main thread alive
        while True:
            sleep(1)
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
        sys.exit(0)