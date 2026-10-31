# 第 12 章：在官方 Superset 镜像上加装 MySQL 驱动 PyMySQL（官方镜像没有带数据库驱动）
# docker compose 第一次启动 superset 服务时会自动按这个文件构建镜像
FROM apache/superset:6.1.0
USER root
# 默认用清华 PyPI 镜像；在国外可以 --build-arg PIP_INDEX_URL=https://pypi.org/simple
ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
RUN uv pip install --python /app/.venv/bin/python --index-url ${PIP_INDEX_URL} pymysql==1.1.1
USER superset
