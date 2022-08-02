NAME_SPACE='project-streamx'
# kubectl create secret docker-registry registry.aibee.cn --docker-server=registry.aibee.cn --docker-username=dockerpull --docker-password='Pha9EGhi#233' -n $NAME_SPACE
helm install streamx streamx/  -n $NAME_SPACE
