FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/lsmdb-node ./cmd/lsmdb-node \
    && CGO_ENABLED=0 go build -o /out/lsmdbctl ./cmd/lsmdbctl

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /out/lsmdb-node /usr/local/bin/lsmdb-node
COPY --from=build /out/lsmdbctl /usr/local/bin/lsmdbctl
ENTRYPOINT ["lsmdb-node"]
