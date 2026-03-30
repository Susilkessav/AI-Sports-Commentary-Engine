# VM Deployment Guide

This guide is tailored to this repository's production shape:

- `caddy` is the only public entrypoint
- `api` serves both the dashboard UI and the WebSocket/API traffic
- `producer`, `enricher`, and `commentator` run as private worker containers
- `redpanda` provides a single-node Kafka-compatible broker on the same VM

## 1. Provision the VM

Use a small Ubuntu 24.04 VM:

- Minimum: 2 GB RAM / 1 vCPU
- Recommended: 4 GB RAM / 2 vCPU if you expect steady traffic or heavier LLM throughput

Open these firewall ports:

- `22/tcp` for SSH
- `80/tcp` for HTTP
- `443/tcp` for HTTPS

Point your DNS `A` record to the VM's public IPv4 address before starting Caddy.

## 2. Install Docker and Compose

SSH into the VM and run:

```bash
sudo apt update
sudo apt install -y ca-certificates curl git
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
newgrp docker
docker --version
docker compose version
```

## 3. Clone the repo

```bash
git clone <your-repo-url>
cd AI-Sports-Commentary-Engine
```

## 4. Create the production env file

```bash
cp .env.production.example .env.production
```

Edit `.env.production` and set at least:

- `DOMAIN`
- `ACME_EMAIL`
- `CORS_ORIGINS`
- `LLM_PROVIDER`
- `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`
- `SPORT`

Recommended first production values:

```dotenv
DOMAIN=commentary.yourdomain.com
ACME_EMAIL=you@yourdomain.com
CORS_ORIGINS=https://commentary.yourdomain.com
LLM_PROVIDER=openai
OPENAI_API_KEY=your_real_key
SPORT=cricket
CRICKET_LEAGUE=ipl
COMMENTARY_STYLE=default
```

## 5. Start the production stack

Build and launch:

```bash
docker compose --env-file .env.production -f docker-compose.prod.yml up -d --build
```

Check container status:

```bash
docker compose --env-file .env.production -f docker-compose.prod.yml ps
```

Tail logs:

```bash
docker compose --env-file .env.production -f docker-compose.prod.yml logs -f api
docker compose --env-file .env.production -f docker-compose.prod.yml logs -f commentator
docker compose --env-file .env.production -f docker-compose.prod.yml logs -f producer
```

## 6. Verify the deployment

Once DNS has propagated and Caddy has issued TLS certs:

```bash
curl https://your-domain.example/health
```

Expected result:

```json
{"status":"ok","connections":0}
```

Then open these in a browser:

- `https://your-domain.example/`
- `https://your-domain.example/api`
- `https://your-domain.example/health`

## 7. Day-2 operations

Update the app:

```bash
git pull
docker compose --env-file .env.production -f docker-compose.prod.yml up -d --build
```

Restart a single service:

```bash
docker compose --env-file .env.production -f docker-compose.prod.yml restart commentator
```

Stop the stack:

```bash
docker compose --env-file .env.production -f docker-compose.prod.yml down
```

## Notes specific to this repo

- The dashboard is already served by `FastAPI` at `/`, so there is no separate dashboard container in production.
- WebSocket traffic is proxied through Caddy to `api`, and the dashboard now auto-selects `ws://` or `wss://` based on the page protocol.
- The producer stores deduplication state in the `producer_data` volume so repeated restarts do not immediately replay old events.
- This stack is single-node by design. It is cost-friendly, but not highly available.

## Recommended next upgrades

When you outgrow a single VM, move in this order:

1. Move Redpanda/Kafka to a managed service.
2. Put `api` behind a managed load balancer.
3. Add centralized logs and metrics.
4. Split workers onto separate machines or containers with autoscaling.
