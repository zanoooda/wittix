FROM golang:1.22-alpine AS builder
WORKDIR /src
RUN apk add --no-cache ca-certificates

# Better layer caching
COPY go.mod go.sum ./
RUN go mod download

# App sources
COPY . .

# Build the entire package
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/app .

FROM gcr.io/distroless/base-debian12
WORKDIR /
COPY --from=builder /bin/app /app
ENV HTTP_ADDR=:8080
USER 65532:65532
ENTRYPOINT ["/app"]