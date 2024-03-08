FROM golang:latest

WORKDIR /otel

COPY . .

RUN go build -o gateway_collector ./otelcol-dev

ENTRYPOINT [ "./gateway_collector" ]

EXPOSE 4317
EXPOSE 4318
EXPOSE 8888
EXPOSE 55678
EXPOSE 55679

CMD ["--config", "configfile.yaml "]