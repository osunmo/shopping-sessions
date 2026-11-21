# Shopping Sessions: single static-ish Rust binary.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
RUN cargo build --release --locked && cp target/release/shopping-sessions /pipeline

FROM debian:bookworm-slim
COPY --from=build /pipeline /app/pipeline
USER 1000:1000
CMD ["/app/pipeline"]
