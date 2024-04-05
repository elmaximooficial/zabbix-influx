pub mod database;

use database::{influx, postgresql};


#[tokio::main]
async fn main() {
    loop{
        let postgres = postgresql::from_file("/etc/influxdb/zabbix-rust/config.toml");
        let influx_client = influx::from_file("/etc/influxdb/zabbix-rust/config.toml");
        tokio::spawn(async move{
            for i in postgres.clone().zabbix_tables{
                let _ = postgresql::sync_postgres(postgres.clone(), 10000_usize, &influx_client, &i).await;
            }
        }).await.unwrap();
    }
}
