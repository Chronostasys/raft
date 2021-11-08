FROM golang:1.17 AS builder
WORKDIR /kvraft
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct
COPY . .
RUN make

FROM alpine:latest
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.ustc.edu.cn/g' /etc/apk/repositories
RUN apk --no-cache add ca-certificates
COPY --from=builder /kvraft/out/kvraft/kvraft /app
COPY --from=builder /kvraft/out/kvraft/cli /cli
ENTRYPOINT /app