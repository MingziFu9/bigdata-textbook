# 第 10 章起：在官方 Spark 镜像上加装 numpy（Spark MLlib 的 Python 接口要用它）
# docker compose 第一次启动 spark 服务时会自动按这个文件构建镜像，约 1 分钟
FROM apache/spark:4.1.4-scala2.13-java21-python3-ubuntu
USER root
# 默认用清华 PyPI 镜像；在国外可以 --build-arg PIP_INDEX_URL=https://pypi.org/simple
ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install --no-cache-dir -i ${PIP_INDEX_URL} numpy==2.2.6
