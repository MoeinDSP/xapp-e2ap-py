export SCHEMA_PATH=schemas
export XAPP_DIR=python_xapp
export DBAAS_SERVICE_HOST=10.0.2.12
export DBAAS_SERVICE_PORT="6379"
export STAGE_DIR="/tmp"
export SCHEMA_FILE=""
export MDC_VER=0.0.4-1
export RMR_VER=4.0.5
export RNIB_VER=1.0.0
export E2AP_VERSION=1.1.0
export RMR_RTG_SVC="9999"
export RMR_SEED_RT="/python_xapp/routes.txt"
export LD_LIBRARY_PATH="/usr/local/lib:/usr/local/libexec"
export VERBOSE=0
export CONFIG_FILE="/opt/ric/config/config-file.json"

# Update and install necessary packages
sudo apt-get update
sudo apt-get install -y git build-essential wget dpkg cmake openssh-server python3-pip

# Install py-plt
cd ${STAGE_DIR}
git clone https://github.com/o-ran-sc/ric-plt-xapp-frame-py.git
cd ric-plt-xapp-frame-py
git checkout e-release
pip3 install .

# Install protobuf
pip3 install protobuf


# Install rmr
cd ${STAGE_DIR}
git clone --branch e-release https://gerrit.oran-osc.org/r/ric-plt/lib/rmr
cd rmr
mkdir .build && cd .build
cmake .. -DDEV_PKG=1
make install
cmake .. -DPACK_EXTERNALS=1
make install
cd ../..
rm -rf rmr

# Install e2ap lib
cd ${STAGE_DIR}
git clone --branch ${E2AP_VERSION} https://github.com/o-ran-sc/ric-plt-libe2ap.git
cd ric-plt-libe2ap
cmake .
make
sudo make install

# Setup SSH server
sudo mkdir -p /root/.ssh
sudo chmod 0700 /root/.ssh
echo 'root:pass' | sudo chpasswd
sudo mkdir -p /run/openrc
sudo touch /run/openrc/softlevel

# Clean up
sudo apt-get remove -y wget dpkg cmake
sudo rm -rf ${STAGE_DIR}/*
