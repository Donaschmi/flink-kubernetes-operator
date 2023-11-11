docker build . -t donaschmitz/flink-kubernetes-operator:latest
helm uninstall flink-kubernetes-operator
kind load docker-image donaschmitz/flink-kubernetes-operator:latest
helm install flink-kubernetes-operator helm/flink-kubernetes-operator --set image.repository=donaschmitz/flink-kubernetes-operator --set image.tag=latest