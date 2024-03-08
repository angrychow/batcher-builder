FROM golang:latest

WORKDIR /app

COPY . .

RUN go build -o gateway_collector ./otelcol-dev

CMD ["./gateway_collector --config config.yaml "]