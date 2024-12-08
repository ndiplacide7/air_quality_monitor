## README

- Real-Time Air Quality Monitoring System using Django, Apache Hadoop, Apache Kafka, and AWS services.






 ## Run the Pipeline

### Terminal 1: Start Redis (Message Broker)
```bash
redis-server
```
### Test Redis connection
```bash 
python -c "import redis; r = redis.Redis(host='localhost', port=6379, db=0); print(r.ping())"
```
### Manual Trigger
```bash
python manage.py run_air_quality_pipeline
```