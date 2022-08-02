docker build  -f deploy/docker/console/Dockerfile -t registry.aibee.cn/data-exchange/streamx:1.2.4 .
docker tag registry.aibee.cn/data-exchange/streamx:1.2.4 registry.aibee.cn/data-exchange/streamx:latest
docker push registry.aibee.cn/data-exchange/streamx:1.2.4
docker push registry.aibee.cn/data-exchange/streamx:latest
