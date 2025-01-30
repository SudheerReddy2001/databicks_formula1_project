# Databricks notebook source
def mount_adls(storage_account_name, container_name, directory_name):
    # Secrets for authentication
    client_id = dbutils.secrets.get(scope = 'su-scope', key = 'su-client-id')
    tenant_id = dbutils.secrets.get(scope = 'su-scope', key = 'su-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'su-scope', key = 'su-secret-id')

    # Spark configuration for OAuth
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }
    
    # Define dynamic mount point
    mount_point = f"/mnt/{storage_account_name}/{container_name}/{directory_name}"
    
    # Check if already mounted and unmount if necessary
    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(mount_point)
    
    # Mount the container
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{directory_name}"
    dbutils.fs.mount(source=source, mount_point=mount_point, extra_configs=configs)
    print(f"Mounted {directory_name} to {mount_point}")

# Define storage account, container, and directory
storage_account_name = "sudheerdatastorage1"
container_name = "sql"
directory_name = "raw"  

# Mount the directory dynamically
mount_adls(storage_account_name, container_name, directory_name)

