# Databricks notebook source
# Databricks notebook source
def mount_adls(storage_account_name, container_name, directory_name):
    
    client_id = dbutils.secrets.get(scope = 'su-scope', key = 'su-client-id')
    tenant_id = dbutils.secrets.get(scope = 'su-scope', key = 'su-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'su-scope', key = 'su-secret-id')

    
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret":client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}/{directory_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}/{directory_name}")
    

    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{directory_name}",
      mount_point = f"/mnt/{storage_account_name}/{container_name}/{directory_name}",
      extra_configs = configs)

# COMMAND ----------

mount_adls("sudheerdatastorage1", "incrementalload", "raw")

# COMMAND ----------

mount_adls("sudheerdatastorage1", "incrementalload", "processed")

# COMMAND ----------

mount_adls("sudheerdatastorage1", "incrementalload", "presentation")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/sudheerdatastorage1/incrementalload/raw"))
