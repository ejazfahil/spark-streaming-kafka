# Makefile 2023-06-01
start-kafka:
	docker-compose up -d zookeeper kafka
submit-job:
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 src/consumer.py
test:
	pytest tests/ -v
