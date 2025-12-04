# Scraper Python + Postgresql
Proyek ini men-setup **PostgreSQL 16** menggunakan Docker Compose, lengkap dengan extension:

- `uuid-ossp`
- `pg_trgm`
- `pgvector`

## Setup Docker + PG16 + PGVector

1. Jalankan Docker Compose
```
docker-compose up -d
```
2. Masuk ke database PostgreSQL:
```
docker exec -it pg16 psql -U admin -d tokopedia
```
3. Enable extension (hanya sekali):
```
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS vector;
```

4. Config dns
C:\Users\<YOUR USER>\.docker\daemon.json
```
"dns": ["8.8.8.8", "8.8.4.4"]
```

## Running

1. Clone 
2. Install dependency
```
pip install -r requirements.txt
```
