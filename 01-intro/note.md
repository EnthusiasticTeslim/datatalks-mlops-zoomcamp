# Setting up Google cloud platform
1. Sign up on Google with gmail account.
2. Connect your payment details and receive $300 credit.
3. Create a project and start a compute engine. feel free to modify accordingly.

# Connect gcp with local desktop
1. Install Remote - SSH (by Microsoft) in VS code.
2. create (in any location) a IdentityFile as follows: \
`ssh-keygen -t rsa -f name-of-IdentityFile -C desired-username -b 2048`
This generates 2 files namely `name-of-IdentityFile` and `name-of-IdentityFile.pub`

3. Press `F1` and type `Remote SSH: Connect to Host`. Select Configure SSH Hosts and fill the following info:

`Host desired-remotename
    HostName xx.xxx.xxx.xx
    User desired-username
    IdentityFile path-to-Identity-file/name-of-IdentityFile`

4. Copy the `HostName` from the External IP address availbe in the GCP Network interface sub-section.

5. Go to the terminal, do `ssh desired-remotename` to enter the remote server

# Clone the mlops-zoom-repo
If you wish to contribute directly from the remote server, connect your Github to the remote server. How?
1. Create a ssh key using `ssh-keygen -t ed25519 -C "yourEmail@mail.com"`. this should create 2 files (`ed25519` & `ed25519.pub`) in the `~/.ssh/` directory
2. Go to your github profile setting, find the SSH and GCP keys option and copy the info in ed25519.pub into the text bar. use any title 
3. do `git clone git@github.com:username/mlops-zoomcamp.git`.

# References
1. SSH in Github: https://www.youtube.com/watch?v=X40b9x9BFGo
2. GCP: https://www.youtube.com/watch?v=0Bjx3Ra8PRM