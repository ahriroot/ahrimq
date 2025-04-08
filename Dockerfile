# ===== 构建阶段 =====
FROM rust:1.85-alpine AS builder

# 安装 musl 编译工具链
RUN apk add --no-cache musl-dev

WORKDIR /app

COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

# ===== 运行时阶段 =====
FROM alpine:latest

# 安装运行时依赖（按需添加）
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# 创建配置文件
RUN touch config.toml

# 从构建阶段复制二进制文件
COPY --from=builder /app/target/release/amqs /usr/local/bin/
COPY --from=builder /app/target/release/amqc /usr/local/bin/

# 设置入口点
ENTRYPOINT ["amqs"]
