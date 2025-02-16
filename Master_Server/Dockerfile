# Build Stage
FROM golang:1.22 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Build with static linking
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o masterserver .

# Final Stage
FROM alpine:latest
WORKDIR /app
# Add required dependencies
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/masterserver .
RUN chmod +x masterserver
EXPOSE 50052
CMD ["./masterserver"]
