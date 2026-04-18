#!/bin/bash
echo "=== 1. 检查应用日志中的连接池错误 ==="
kubectl logs -n production deployment/api-server --tail=100 | grep -i "pool\|connection\|timeout\|hikari"

echo -e "\n=== 2. 检查当前连接池配置 ==="
kubectl get configmap api-config -n production -o yaml | grep -i "pool\|db_pool"

echo -e "\n=== 3. 检查环境变量配置 ==="
kubectl get deployment api-server -n production -o yaml | grep -A5 -B5 -i "pool\|db_pool"

echo -e "\n=== 4. 检查运行中Pod的实际环境变量 ==="
POD=$(kubectl get pods -n production -l app=api-server -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n production $POD -- env | grep -i "pool\|db"

echo -e "\n=== 5. 对比最近的ConfigMap变更 ==="
kubectl get events -n production --field-selector reason=ConfigMapUpdated --sort-by='.lastTimestamp' | tail -5