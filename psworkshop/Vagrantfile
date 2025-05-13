Vagrant.configure("2") do |config|
  config.vm.box = "generic/debian11"

  config.vm.box_check_update = true

  config.vm.network "public_network", use_dhcp_assigned_default_route: true
  config.vm.synced_folder "./config", "/config"
  config.vm.provision "file", source: "./prestashop_1.7.6.4.zip", destination: "prestashop_1.7.6.4.zip"

  config.vm.provider "virtualbox" do |vb|
    vb.cpus  = 4
    vb.memory = "8192"
  end

  config.vm.provision "shell", path: "bootstrap.sh"

end
