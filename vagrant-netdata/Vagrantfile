# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|

  config.vm.provider :libvirt do |libvirt|
    # otherwise it defaults to qemu://localhost/system?...
    libvirt.uri = "qemu:///system"
  end

  config.vm.define "prometheus", primary: true do |prom|
    prom.vm.box = "centos/8"
    prom.vm.network "public_network", dev: "virbr0", mode: "bridge", type: "bridge"
    prom.vm.network "private_network", ip: "10.21.30.40", libvirt__network_name: "vagrant-net2"
  end

  N = 1
  
  (1..N).each do |machine_id|

    config.vm.define "netdata-#{machine_id}" do |netdata|
      netdata.vm.box = "centos/8"
      netdata.vm.network "public_network", dev: "virbr0", mode: "bridge", type: "bridge"
      netdata.vm.network "private_network", ip: "10.21.30.4#{machine_id}", libvirt__network_name: "vagrant-net2"
      if machine_id == N

        netdata.vm.provision :ansible do |ansible|
          ansible.limit = "all"
          ansible.playbook = "playbook.yml"
          ansible.groups = {
            "netdata" => ["netdata-[1:#{machine_id}]"]
          }
        end
      
      end

    end

  end

end
