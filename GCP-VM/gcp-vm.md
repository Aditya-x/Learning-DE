# How to Use GCP VM on Your Local Machine

This document provides a step-by-step guide on how to create a Google Cloud Platform (GCP) Virtual Machine (VM) instance and connect to it from your local machine using SSH. It covers the creation of a VM instance, generating an SSH key pair, retrieving the public key, and establishing a connection to the VM instance.

## 1.1 Create a GCP VM Machine Instance

To create a VM instance on GCP, follow these steps:

#### Log in to Google Cloud Console:

- Go to [Google Cloud Console](https://console.cloud.google.com/).
- Sign in with your Google account.

#### Select or Create a Project:

- Choose an existing project or create a new one by clicking on the project dropdown at the top of the page.

#### Navigate to the Compute Engine:

- In the left sidebar, click on **"Compute Engine"** and then select **"VM instances"**.

#### Create a New Instance:

1. Click on the **"Create Instance"** button.
2. Fill in the required details:
   - **Name**: Give your instance a name.
   - **Region and Zone**: Choose the desired region and zone.
   - **Machine Configuration**: Select the machine type based on your requirements.
   - **Boot Disk**: Choose the operating system and version you want to use.
   - **Firewall**: Check the boxes to allow HTTP and HTTPS traffic if needed.

#### Create the Instance:

- Once all configurations are set, click on the **"Create"** button to launch your VM instance.

## 1.2 Create an SSH Pair on Your Local Machine

To connect to your GCP VM instance, you need to create an SSH key pair on your local machine:

#### Open Terminal:

- On your local machine, open a terminal (**Command Prompt**, **PowerShell**, or **Terminal on macOS/Linux**).

#### Generate SSH Key Pair:

Run the following command to generate an SSH key pair:

```sh
ssh-keygen -t rsa -f ~/.ssh/gcp_vm_key -C "your_email@example.com"

```

Press Enter to accept the default file location and optionally provide a passphrase.

## 1.3 Locate the Public Key: View the public key using:
```sh
Copy
cat ~/.ssh/gcp_vm_key.pub
```

#### Add the Public Key to Your VM Instance
-   Access the Instance:

-  **Go to the VM instances** page in the GCP Console and click on the name of your instance.
Edit the Instance:

-  Click "Edit" at the top of the instance details page.
-  Add SSH Key:

-   Scroll to the "SSH Keys" section, click "Add item", and paste the public key from *~/.ssh/gcp_vm_key.pub.*
-   Save Changes:

-   Click "Save".
##  1.4 Connect to the VM Instance
-  SSH into the VM: Run the following command in your terminal:

```sh
ssh -i ~/.ssh/gcp_vm_key username@external-ip-address
```
-  Replace username with your desired username.
-  Replace external-ip-address with your VM's external IP address (found in the GCP instance details).
Accept the Connection:

-  Type "yes" if prompted to confirm the connection.


### Extra:
-  To automate connection using a config file you can createa a config file with below code and run it directly from terminal to connect to your VM instance
``` sh
Host de-zoomcamp
    HostName External-GCP-VM key
    User user
    IdentityFile C:\Users\.ssh\gcp-ssh

```