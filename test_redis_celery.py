import os
import sys
import django
import redis
from celery import Celery

# Setup Django
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'air_quality_monitor.settings')
django.setup()

# Initialize Celery Application
app = Celery('air_quality_monitor')
app.config_from_object('django.conf:settings', namespace='CELERY')


# Redis Connection Test
def test_redis_connection():
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        print("Redis Ping Result:", r.ping())
        return True
    except Exception as e:
        print(f"Redis Connection Error: {e}")
        return False


# Celery App Test
def test_celery_app():
    try:
        app = Celery('air_quality_monitor')
        app.config_from_object('django.conf:settings', namespace='CELERY')
        print("Celery App Configured Successfully")
        return True
    except Exception as e:
        print(f"Celery Configuration Error: {e}")
        return False


# Simple Test Task
@app.task(bind=True, name='air_quality_pipeline.tasks.debug_task')
def debug_task(self):
    print(f'Request: {self.request!r}')


def main():
    print("Starting Diagnostic Tests...")

    redis_ok = test_redis_connection()
    celery_ok = test_celery_app()

    if redis_ok and celery_ok:
        print("All tests passed! Services are configured correctly.")
    else:
        print("Some tests failed. Please check your configuration.")


if __name__ == '__main__':
    main()
