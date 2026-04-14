.PHONY: build run up down load-test load-test-bulk

# Application commands
build:
	mvn clean package -DskipTests

run: build
	java -jar target/event-ingestion-0.0.1-SNAPSHOT.jar

# Setup commands using parent docker-compose
up:
	docker-compose up -d

down:
	docker-compose down

# Test commands
load-test:
	go run ../scripts/loadtest.go -mode=single

load-test-bulk:
	go run ../scripts/loadtest.go -mode=bulk
