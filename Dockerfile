FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/crmbridge ./cmd/crmbridge \
 && CGO_ENABLED=0 go build -o /out/fakes ./cmd/fakes

FROM alpine:3.21
RUN adduser -D -u 10001 app && mkdir /data && chown app /data
COPY --from=build /out/ /usr/local/bin/
USER app
WORKDIR /data
EXPOSE 8788
ENTRYPOINT ["/usr/local/bin/crmbridge"]
CMD ["-c", "/data/config.yaml"]
