FROM python:3.8.16-slim-buster

ENV DEBIAN_FRONTEND noninteractive
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV _DEPLOY=1
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo 'Asia/Shanghai' >/etc/timezone
RUN sed -i s/deb.debian.org/mirrors.aliyun.com/g /etc/apt/sources.list \
    && apt-get -qq update \
    && apt-get install -yq sudo vim gcc \
    && rm -rf /var/lib/apt/lists/*
# gcc for apscheduler build

## create a non-root user
ARG USER_ID=1000
RUN useradd -m --no-log-init --system  --uid ${USER_ID} appuser -g sudo \
    && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER appuser
ENV PATH="/home/appuser/.local/bin:${PATH}"

WORKDIR /home/appuser/project/
COPY --chmod=777 ./ /home/appuser/project/

RUN /usr/local/bin/python3 -m pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple \
    && pip install -r ./requirements.txt --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple && pip cache purge

CMD ["python", "main.py"]