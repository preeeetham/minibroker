# Live demo image (mb-web): the broker, a simulated fleet and the dashboard
# in one small binary. Used by Render (render.yaml).
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --features web --bin mb-web

FROM debian:bookworm-slim
RUN useradd --create-home --uid 10001 app
COPY --from=build /src/target/release/mb-web /usr/local/bin/mb-web
USER app
ENV PORT=8080 \
    RUST_LOG=info,minibroker=warn
EXPOSE 8080
CMD ["mb-web"]
