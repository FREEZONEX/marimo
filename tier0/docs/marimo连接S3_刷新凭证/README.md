# Marimo 连接 S3 Tables - IRSA 凭证刷新方案

## ⚠️ 方案状态

**本方案已废弃，仅作为历史记录保留。**

当前生产环境使用的是 **AK/SK 静态凭证方案**，详见 `../marimo连接S3_AKSK/` 目录。

## 📌 方案说明

这是之前使用的方案，通过 IRSA (IAM Roles for Service Accounts) 获取临时凭证，并使用后台线程定期刷新凭证。

### ❌ 废弃原因

1. **配置复杂**: 需要配置 ServiceAccount、IAM Role、Trust Policy 等
2. **稳定性问题**: credential_chain 在某些环境下失败，需要 fallback 到 boto3
3. **内存泄漏**: 后台刷新线程可能导致内存持续增长，最终 OOM
4. **调试困难**: credential chain 失败原因不明确，排查困难

### 📊 遇到的问题

- **Pod 被 Killed**: 频繁刷新凭证导致内存泄漏，Pod 被 OOM Killed
- **凭证刷新失败**: credential_chain 配置问题导致凭证刷新失败
- **多线程冲突**: sitecustomize.py 和 tier0_s3tables.py 都启动刷新线程，导致资源浪费

## 📂 文件说明

- **部署方案.md**: 原 IRSA 方案的部署文档（已添加废弃说明）
- **问题排查-Pod被Killed.md**: Pod OOM 问题的排查过程（已添加废弃说明）
- **Marimo_OOM_Fix.md**: OOM 问题的修复尝试（已添加废弃说明）
- **IRSA_boto3_Plan/**: 包含 IRSA 方案的代码实现

## 🔄 迁移到新方案

如果你还在使用本方案，建议迁移到 AK/SK 方案：

1. 准备 AWS Access Key ID 和 Secret Access Key
2. 创建 Kubernetes Secret 存储凭证
3. 更新 Deployment 配置，使用新的环境变量
4. 使用新的镜像（基于 AK/SK 方案构建）
5. 重启 Pod

详细步骤请参考 `../marimo连接S3_AKSK/部署方案.md`

## 📞 支持

如有问题，请参考新方案文档：`../marimo连接S3_AKSK/部署方案.md`
