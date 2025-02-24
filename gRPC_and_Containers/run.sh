docker build . -f Dockerfile.cache -t p2-cache
docker build . -f Dockerfile.dataset -t p2-dataset
export PROJECT=p2
docker compose up -d
autobadger --project=p2 --verbose