#!/usr/bin/env bash

# export DBHOST=localhost
export ADM_EMAIL=me@example.com
export ADM_PASSWD=testing
export COUNTRY_LANG=fr
export SHOP_DOMAIN=prestaworkshop.com
export DBNAME=prestaworkshop
export DBUSER=prestaworkuser
export DBPASSWD="ThisIsFuck****Insecure"
export PS_VERSION=prestashop_1.7.6.4

apt-get update
export DEBIAN_FRONTEND=noninteractive 
apt-get upgrade -y
apt-get -y install vim curl unzip build-essential ca-certificates lsb-release apt-transport-https gettext

# add php repos

sh -c 'echo "deb https://packages.sury.org/php/ $(lsb_release -sc) main" > /etc/apt/sources.list.d/php.list'
wget -qO - https://packages.sury.org/php/apt.gpg | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/sury_php.gpg
sed -i 's|deb|deb [signed-by=/etc/apt/trusted.gpg.d/sury_php.gpg]|g' /etc/apt/sources.list.d/php.list

apt-get update

# install mysql

debconf-set-selections <<< "mysql-server mysql-server/root_password password $DBPASSWD"
debconf-set-selections <<< "mysql-server mysql-server/root_password_again password $DBPASSWD"

apt-get -y install mariadb-server

mysql -uroot -p"$DBPASSWD" -e "CREATE DATABASE $DBNAME COLLATE utf8mb4_general_ci"
mysql -uroot -p"$DBPASSWD" -e "grant all privileges on $DBNAME.* to '$DBUSER'@'%' identified by '$DBPASSWD'"
mysql -uroot -p"$DBPASSWD" -e "FLUSH PRIVILEGES"
service mysql restart

# setup webserver
apt-get -y install apache2 libapache2-mod-php7.2 php7.2 php7.2-common php7.2-curl php7.2-gd php7.2-imagick php7.2-mbstring php7.2-mysql php7.2-json php7.2-xsl php7.2-intl php7.2-zip php7.2-soap

a2enmod ssl
a2enmod rewrite

mkdir -p /etc/apache2/ssl
openssl req -subj "/CN=$SHOP_DOMAIN/O=PWS/C=FR" -new -newkey rsa:2048 -sha256 -days 365 -nodes -x509 -keyout /etc/apache2/ssl/cert.key -out /etc/apache2/ssl/cert.crt

sed -i "s/^ServerTokens OS/ServerTokens Major/g" /etc/apache2/conf-available/security.conf

# deploy prestashop sources
cd /vagrant
unzip $PS_VERSION.zip
rm -f index.php
rm -f /var/www/html/index.html
unzip prestashop.zip
rsync -a --chown=www-data:www-data --chmod=Du=rwx,Dg=rx,Do=rx,Fu=rw,Fg=r,Fo=r ./ /var/www/html

# configure vhost
envsubst '$SHOP_DOMAIN' < /config/vhost.conf > /etc/apache2/sites-available/000-default.conf
cp -f /config/php.ini /etc/php/7.2/apache2/php.ini

service apache2 restart

# install prestashop via cli
cd /var/www/html/

sudo -u www-data php install/index_cli.php \
--domain="$SHOP_DOMAIN" \
--db_server=localhost \
--db_name="$DBNAME" \
--db_user="$DBUSER" \
--db_password="$DBPASSWD" \
--prefix=pws_ \
--email="$ADM_EMAIL" \
--password="$ADM_PASSWD" \
--language="$COUNTRY_LANG" \
--country="$COUNTRY_LANG" \
--ssl=1 \
--rewrite=1 \
--activity=7

rm -rf ./install

LOCAL_IP=$(ip addr show eth1 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1)
echo -e "\nAdd this to your /etc/hosts file to access the site\n$LOCAL_IP $SHOP_DOMAIN"
