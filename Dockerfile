FROM python:3.12-slim

ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /quant3

# 依赖层（利用 Docker 缓存，依赖不变时跳过 pip install）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 代码层
COPY . .

# daemon 8502 / Streamlit 8501
EXPOSE 8501 8502

# 数据库持久化目录
VOLUME /quant3/data

# 默认启动守护进程（docker-compose 会覆盖 command）
CMD ["python", "db_daemon.py"]
