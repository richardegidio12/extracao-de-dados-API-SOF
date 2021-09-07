#!/usr/bin/expect
/usr/bin/mysql --login-path=awslogin <<EOFMYSQL
LOAD DATA LOCAL INFILE '/home/ec2-user/sua_pasta/contratos_diadacoleta.csv' 
INTO TABLE schema_contratos.contratos FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;
EOFMYSQL
echo "Carregamento de dados no banco deu certo!"
