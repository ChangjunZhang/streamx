docker build  -f deploy/docker/compose/Dockerfile -t registry.aibee.cn/data-exchange/streamx:1.2.4-dev .
docker tag registry.aibee.cn/data-exchange/streamx:1.2.4-dev registry.aibee.cn/data-exchange/streamx:latest
docker push registry.aibee.cn/data-exchange/streamx:1.2.4-dev
docker push registry.aibee.cn/data-exchange/streamx:latest
