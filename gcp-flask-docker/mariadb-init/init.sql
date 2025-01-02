-- init.sql

-- 'flask_user'@'localhost' 사용자 생성 및 권한 부여
CREATE USER 'flask_user'@'localhost' IDENTIFIED BY 'flask_password123!';
GRANT ALL PRIVILEGES ON testdb.* TO 'flask_user'@'localhost';
FLUSH PRIVILEGES;
