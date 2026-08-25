set dotenv-load
set default-list

run:
    @echo "Running Jetstream..."
    whoami
    CGO_ENABLED=1 GOOS=linux go run cmd/jetstream/*.go
