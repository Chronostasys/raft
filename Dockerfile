FROM golang:1.17 AS builder
RUN make

FROM alpine:latest
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.ustc.edu.cn/g' /etc/apk/repositories
RUN apk --no-cache add ca-certificates
COPY --from=builder out/kvraft/kvraft /app
COPY --from=builder out/kvraft/cli /cli
ENTRYPOINT /app