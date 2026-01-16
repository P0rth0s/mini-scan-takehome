FROM golang:1.20-alpine AS builder

# Install build dependencies for Alpine/musl
RUN apk add --no-cache gcc musl-dev

# Build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o subscriber ./cmd/subscriber

# Copy binary into image
FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /src/subscriber .
CMD ["/app/subscriber"]