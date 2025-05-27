Recommended development environment: Linux

### Step 1: Download and install the Anaconda distribution of Python
```sh
wget https://repo.anaconda.com/archive/Anaconda3-2022.05-Linux-x86_64.sh
bash Anaconda3-2022.05-Linux-x86_64.sh
```

### Step 2: Update existing packages

```sh
sudo apt update
```

### Step 3: Install Docker

```sh
sudo apt install docker.io
```

To run docker without `sudo`:

```sh
sudo groupadd docker
sudo usermod -aG docker $USER
```

### Step 4: Install Docker Compose

Install docker-compose in a separate directory

```sh
mkdir soft
cd soft
```

To get the latest release of Docker Compose, go to https://github.com/docker/compose and download the release for your OS.

```sh
wget https://github.com/docker/compose/releases/download/v2.5.0/docker-compose-linux-x86_64 -O docker-compose
```

Make it executable

```sh
chmod +x docker-compose
```

Add to the `soft` directory to `PATH`. Open the `.bashrc` file with `nano`:

```sh
nano ~/.bashrc
```

In `.bashrc`, add the following line:

```bash
export PATH="${HOME}/soft:${PATH}"
```

Save it and run the following to make sure the changes are applied:

```bash
source ~/.bashrc
```


### Step 5: Run Docker

```sh
docker run hello-world
```

If you get `docker: Got permission denied while trying to connect to the Docker daemon socket at unix:///var/run/docker.sock: Post "http://%2Fvar%2Frun%2Fdocker.sock/v1.24/containers/create": dial unix /var/run/docker.sock: connect: permission denied.` error, restart your VM instance. 


**Note**: If you get `It is required that your private key files are NOT accessible by others. This private key will be ignored.` error, you should change permits on the downloaded file to protect your private key:

 ```sh
chmod 400 name-of-your-private-key-file.pem
```

