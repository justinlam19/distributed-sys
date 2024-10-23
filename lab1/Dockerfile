# Build the application from source
FROM golang:1.23 AS build-stage

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /video-rec-service ./video_rec_service/server

FROM build-stage AS run-test-stage
RUN go test -v ./...

FROM alpine:latest AS build-release-stage

WORKDIR /

COPY --from=build-stage video-rec-service /video-rec-service

EXPOSE 8080

ENTRYPOINT ["/video-rec-service"]
