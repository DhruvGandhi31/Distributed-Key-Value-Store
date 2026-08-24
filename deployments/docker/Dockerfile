# Multi-stage build: compile in a full Go image, ship only the static
# binaries in a distroless runtime so the final image has no shell, no
# package manager, nothing beyond what kvstored actually needs.

FROM golang:1.23-alpine AS builder
WORKDIR /build
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /kvstored ./cmd/server
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /kvctl ./cmd/client

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /kvstored /kvstored
COPY --from=builder /kvctl /kvctl
EXPOSE 7000 8000
VOLUME ["/data"]
ENTRYPOINT ["/kvstored"]
