# Go screening worker (Phase 10 [STRETCH]) — drop-in for the Python worker.
# Build context: ./go-screening
FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -o /go-screening .

FROM gcr.io/distroless/static-debian12
COPY --from=build /go-screening /go-screening
ENTRYPOINT ["/go-screening"]
