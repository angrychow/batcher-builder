FROM golang:latest

WORKDIR /app

COPY . .

RUN go build -o agent_collector ./otelcol-dev

CMD ["./agent_collector --config config.yaml "]