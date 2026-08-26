set dotenv-load
set default-list

JETSTREAM_VERSION := `git rev-parse HEAD`

run:
    @echo "Running Jetstream..."
    whoami
    CGO_ENABLED=1 GOOS=linux go run cmd/jetstream/*.go

build-image-amd64:
    @echo "Building docker image for amd64..."
    docker buildx build --platform linux/amd64 -f Dockerfile -t jetstreamproxy:{{ JETSTREAM_VERSION }}-amd64 --load .
