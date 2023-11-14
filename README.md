## 1. Configure Developer Environment
    
    devcontainer build .
    devcontainer open .
    cp .env.template .env

## 2 Install and configure yc CLI


## 2.1 Initialization yc

    yc init

## 2.2 Set environment variables

    export YC_TOKEN=$(yc iam create-token)
    export YC_CLOUD_ID=$(yc config get cloud-id)
    export YC_FOLDER_ID=$(yc config get folder-id)
    export $(xargs <.env)
    
## 4. Deploy Clickhouse Cluster

## 4.1 Terraform

    cp terraformrc ~/.terraformrc
    
    terraform init
    terraform validate
    terraform fmt
    terraform plan
    terraform apply

## 4.2 Store terraform output values as Environment Variables 

    export CLICKHOUSE_HOST=$(terraform output -raw clickhouse_host_fqdn)
    export DBT_HOST=${CLICKHOUSE_HOST}
    export DBT_USER=${CLICKHOUSE_USER}
    export DBT_PASSWORD=${TF_VAR_clickhouse_password}

## 5 Deploy DWH

## 5.1 Check database connection

    dbt debug

## 5.2 Install dbt packages
    
    dbt deps

## 5.3 Stage data sources with dbt macro

    dbt run-operation init_s3_sources
    
## 5.4 Build staging models:
   
    dbt build -s stg_sales

## 5.5 Prepare monthly dynamic RFM table 2023

    
     dbt build -s int_rfm --vars 'dt: 2023-01-01'
     dbt build -s int_rfm --vars 'dt: 2023-02-01'
     dbt build -s int_rfm --vars 'dt: 2023-03-01'
     dbt build -s int_rfm --vars 'dt: 2023-04-01'
     dbt build -s int_rfm --vars 'dt: 2023-05-01'
     dbt build -s int_rfm --vars 'dt: 2023-06-01'
     dbt build -s int_rfm --vars 'dt: 2023-07-01'
     dbt build -s int_rfm --vars 'dt: 2023-08-01'
     dbt build -s int_rfm --vars 'dt: 2023-09-01'
     dbt build -s int_rfm --vars 'dt: 2023-10-01'
     dbt build -s int_rfm --vars 'dt: 2023-11-01'


     dbt build -s f_rfm

## 6 Shut down Clickhouse cluster

    terraform destroy