FROM mariadb:10.3.29

# my.cnf 복사 및 권한 설정
COPY ./config/my.cnf /etc/mysql/my.cnf
RUN chmod 644 /etc/mysql/my.cnf

# 암호화 키 파일 생성 및 권한 설정
COPY ./encryption/keyfile /etc/mysql/encryption/keyfile
RUN mkdir -p /etc/mysql/encryption && \
    chown -R mysql:mysql /etc/mysql/encryption && \
    chmod -R 600 /etc/mysql/encryption/keyfile

# 데이터 디렉터리 권한 설정
RUN chown -R mysql:mysql /var/lib/mysql && chmod -R 750 /var/lib/mysql

CMD ["mysqld"]
