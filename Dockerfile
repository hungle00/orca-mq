# ==========================================
# STAGE 1: Build Release
# ==========================================
FROM elixir:1.19-alpine AS base

FROM base AS builder

# Install packages required for compilation.
RUN apk add --no-cache build-base git

# Set the working directory inside the container.
WORKDIR /app

# Install Hex and Rebar.
RUN mix local.hex --force && \
    mix local.rebar --force

# Set the production environment for the build.
ENV MIX_ENV=prod

# Copy dependency files first to take advantage of Docker layer caching.
COPY mix.exs mix.lock ./
RUN mix deps.get --only prod
RUN mix deps.compile

# Copy the application source code.
COPY lib lib

# Build the application as a lightweight standalone release.
RUN mix release

# ==========================================
# STAGE 2: Runtime Image (Lightweight)
# ==========================================
FROM base AS runner

# Install runtime dependencies (openssl, ncurses, and libstdc++).
RUN apk add --no-cache libstdc++ ncurses-libs openssl

WORKDIR /app

# Create a non-root user (production security best practice).
RUN adduser -D -h /app orcamq && \
    chown -R orcamq:orcamq /app

USER orcamq

# Copy the release built in stage 1 into stage 2.
COPY --from=builder --chown=orcamq:orcamq /app/_build/prod/rel/orca_mq ./

# Set default environment variables (can be overridden when running the container).
ENV PORT=5555
ENV REPLACE_OS_VARS=true

# Expose the TCP port used by the Ranch server.
EXPOSE 5555

# Start the application in the foreground so Docker can manage its lifecycle.
CMD ["/app/bin/orca_mq", "start"]
