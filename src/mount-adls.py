# Databricks notebook source
# Client - 040064de-9cc0-41a6-8ce6-4dae2fa7f1f0
# Objected - ac5c994a-d6a2-4084-b36d-c0710d88dd48 
# Directory - f61a648c-99c6-4877-abd7-83eae3682c36
# Secret  - DbY8Q~r3NCyX5pACJMNkO9L_nsoFDnfW1ksF-b8q

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "c433564c-b977-449c-9459-397f9227616a",
    "fs.azure.account.oauth2.client.secret": "w2C8Q~v2_C0~8HSMD79tvACDrpKEwkLusJcJsaqo",
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/f61a648c-99c6-4877-abd7-83eae3682c36/oauth2/token",
    "fs.azure.createRemoteFileSystemDuringInitialization": "true"
}

# service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>")

# Replace with your values
adlsAccountName = "singamgen2"
adlsContainerName = "predictiveanalytics"
mountPoint = "/mnt/predictiveanalytics"

dbutils.fs.mount(
  source = f"abfss://{adlsContainerName}@{adlsAccountName}.dfs.core.windows.net/",
  mount_point = mountPoint,
  extra_configs = configs)

