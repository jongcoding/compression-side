FROM mongo:6.0

# 데이터 디렉터리 권한 설정
RUN chown -R mongodb:mongodb /data/db && chmod -R 700 /data/db

CMD ["mongod"]
