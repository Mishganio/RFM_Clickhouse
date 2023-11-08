variable "admin_password" {
  description = "Clickhouse admin password"
  type        = string
  sensitive   = true
#   default     = "password"
}

data "yandex_mdb_clickhouse_cluster" "rfm_clickhouse" {
  name = "rfm_clickhouse"
}

output "clickhouse_host_fqdn" {
  value = data.yandex_mdb_clickhouse_cluster.rfm_clickhouse.host[0].fqdn
}
