# MinIO n'est plus publié sur Docker Hub ni sur quay.io sans authentification :
# on compile le serveur et le client `mc` depuis les sources, à une version figée.
FROM golang:1.24-alpine AS build
ARG MINIO_VERSION=RELEASE.2025-04-22T22-12-26Z
ARG MC_VERSION=RELEASE.2025-04-16T18-13-26Z
RUN apk add --no-cache git
ENV CGO_ENABLED=0 GOBIN=/out
RUN go install github.com/minio/minio@${MINIO_VERSION} \
 && go install github.com/minio/mc@${MC_VERSION}

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl
COPY --from=build /out/minio /out/mc /usr/local/bin/
EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/minio"]
CMD ["server", "/data", "--console-address", ":9001"]
