#!/bin/bash
echo "=== 修复连接池配置 ==="

# 1. 备份当前配置
kubectl get configmap api-config -n production -o yaml > api-config-backup-$(date +%Y%m%d-%H%M).yaml

# 2. 更新连接池大小（从1或5改为20）
kubectl patch configmap api-config -n production -p '{"data":{"DB_POOL_SIZE":"20"}}'

# 3. 重启deployment使配置生效
kubectl rollout restart deployment/api-server -n production

# 4. 等待rollout完成
kubectl rollout status deployment/api-server -n production

# 5. 验证新Pod的配置
echo -e "\n=== 验证修复结果 ==="
POD=$(kubectl get pods -n production -l app=api-server -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n production $POD -- env | grep DB_POOL_SIZE