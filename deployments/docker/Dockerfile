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
# distroless:nonroot runs as uid 65532 with no shell to mkdir/chown at
# runtime, and a freshly created named volume is seeded from whatever's
# at the mountpoint in the image — so the dir has to exist with the right
# owner here, or the first write into /data fails with permission denied.
RUN mkdir -p /data && chown -R 65532:65532 /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder --chown=65532:65532 /data /data
COPY --from=builder /kvstored /kvstored
COPY --from=builder /kvctl /kvctl
EXPOSE 7000 8000
VOLUME ["/data"]
ENTRYPOINT ["/kvstored"]
