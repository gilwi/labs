# psworkshop

## Requirements

- virtualbox
- vagrant
- tested only with prestahsop 1.7

## How to use

1. Clone repo
```
git clone https://github.com/gilwi/psworkshop.git
cd psworkshop
```

3. Download prestashop version 1.7.X.X of your choice\
See available versions [here](https://github.com/PrestaShop/PrestaShop/releases)
```
wget https://github.com/PrestaShop/PrestaShop/releases/download/1.7.X.X/prestashop_1.7.X.X.zip
```

3. Set version & run vagrant
```
sed -i 's/PS_VERSION=.*/PS_VERSION=prestashop_1.7.X.X/g' bootstrap.sh
vagrant up
```