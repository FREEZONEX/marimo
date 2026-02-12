# Marimo 连接 S3 Tables - AK/SK 方案

## 📌 方案说明

这是 **当前生产环境使用的方案**，使用 AWS Access Key/Secret Key 静态凭证连接 S3 Tables。

### ✅ 方案优势

- **配置简单**: 只需设置环境变量，无需配置 ServiceAccount、IAM Role 等
- **稳定可靠**: 凭证长期有效，无需后台刷新线程，避免内存泄漏
- **易于调试**: 凭证问题一目了然，不涉及复杂的 credential chain
- **资源占用低**: 无后台线程，内存占用更低

### ⚠️ 注意事项

- 需要妥善保管 AWS 凭证，建议使用 Kubernetes Secret 存储
- 凭证需要有访问 S3 Tables 的权限
- 如果凭证泄露，需要立即更换

## 📂 文件说明

- **部署方案.md**: 完整的部署文档，包含架构设计、实现细节和部署步骤

## 🔗 相关资源

- **源码仓库**: [FREEZONEX/marimo](https://github.com/FREEZONEX/marimo)
- **开发分支**: `tier0-patch`
- **旧方案文档**: 见 `../marimo连接S3_刷新凭证/` 目录（已废弃）

## 🚀 快速开始

1. 阅读 [部署方案.md](./部署方案.md)
2. 准备 AWS 凭证（Access Key ID 和 Secret Access Key）
3. 按照文档配置环境变量
4. 构建并部署镜像

## 📞 支持

如有问题，请参考 [部署方案.md](./部署方案.md) 中的故障排查章节。
