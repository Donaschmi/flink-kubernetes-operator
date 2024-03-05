kubectl patch flinkdeployment.flink.apache.org flink -p '{"metadata":{"finalizers":null}}' --type=merge
kubectl delete -f nexmark.yaml
helm uninstall flink-kubernetes-operator
kubectl delete crd/flinkdeployments.flink.apache.org
kubectl delete crd/flinksessionjobs.flink.apache.org