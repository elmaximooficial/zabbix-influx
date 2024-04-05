pub mod influx{
    use toml;
    use std::fs;
    use std::path::Path;
    use serde::{Serialize, Deserialize};
    use influxdb::{InfluxDbWriteable, Client, WriteQuery};
    use chrono::{DateTime, Utc};

    #[derive(InfluxDbWriteable, Debug, Clone)]
    pub struct Metric{
        pub time: DateTime<Utc>,
        #[influxdb(tag)]pub endpoint: String,
        #[influxdb(tag)]pub groupid: u32,
        #[influxdb(tag)]pub host_group: String,
        #[influxdb(tag)]pub ip: String,
        #[influxdb(tag)]pub dns: String,
        #[influxdb(tag)]pub port: String,
        #[influxdb(ignore)]pub key: String,
        pub value: f64
    }
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct InfluxDB {
        pub influx_server: String,
        pub influx_port: Option<u16>,
        pub influx_token: String,
        pub influx_bucket: String,
        pub influx_org: String
    }
    pub fn from_file(filepath: &str) -> influxdb::Client{
        let influx_config: InfluxDB = toml::from_str(fs::read_to_string(Path::new(filepath)).unwrap().as_str()).unwrap();
        let influx_client = influxdb::Client::new(
            format!("http://{server}:{port}/api/v2", 
                        server = influx_config.influx_server.as_str(),
                        port = &influx_config.influx_port.unwrap_or(8086)), 
                        &influx_config.influx_bucket,
                        &influx_config.influx_org)
                .with_token(&influx_config.influx_token);
        return influx_client
    }

    pub async fn write_into_influx(data: WriteQuery, client: &Client) -> bool{
        let result = match client.query(data).await{
            Ok(_result) => true,
            Err(e) => {
                eprintln!("Error inserting into InfluxDB {}", e);
                false
            }
        };
        result
    }
    pub async fn write_many_into_influx(data: Vec<WriteQuery>, client: &influxdb::Client) -> bool{
        let result = match client.query(data).await {
            Ok(_result) => true,
            Err(e) => {
                eprintln!("Error Inserting into InfluxDB {}", e);
                false
            }
        };
        result
    }
}
pub mod postgresql{
    use toml;
    use std::fs;
    use std::path::Path;
    use serde::{Serialize, Deserialize};
    use tokio_postgres::{NoTls, Row};
    use chrono::{Utc, TimeZone};
    use rust_decimal::{Decimal, prelude::ToPrimitive};
    use influxdb::{WriteQuery, Timestamp};

    use crate::database;

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct PostgreSQL{
        pub zabbix_server: Vec<String>,
        pub zabbix_port: Option<u16>,
        pub zabbix_username: String,
        pub zabbix_password: String,
        pub zabbix_tables: Vec<String>
    }

    pub fn from_file(filepath: &str) -> PostgreSQL{
        let postgresql = toml::from_str(fs::read_to_string(Path::new(filepath)).unwrap().as_str()).unwrap();
        postgresql
    }

    fn read_last_run(last_run_path: &str) -> i64{
        return std::fs::read_to_string(last_run_path).unwrap().parse::<i64>().unwrap();
    }
  
    fn set_last_run(last_run: i64, last_run_path: &str){
        std::fs::write(last_run_path, (last_run - 50).to_string()).unwrap();
    }

    fn row_to_query(row: Row, history_name: &str) -> WriteQuery{
        let clock: i32 = row.get(2);
        let groupid: i64 = row.get(8);
        let value = match history_name{
            "history_uint" => {
                let dec: Decimal = row.get(3);
                dec.to_f64().unwrap()
            },
            "history" => row.get::<usize, f64>(3),
            _ => panic!("Invalid History name")
        };
        let endpoint = row.get::<usize, String>(0).replace([' ', '-'], "").to_uppercase();
        let host_group = row.get::<usize, String>(7).replace([' ', '-'], "").to_uppercase();
        let dns: String = match row.get(5) {
            "" => std::string::String::from("None"),
            _ => row.get(5)
        };
        let mut query = WriteQuery::new(
            Timestamp::Seconds(
                Utc.timestamp_opt(clock as i64, 0).unwrap().timestamp() as u128), 
            row.get::<usize, String>(1));
        query = query.add_tag("dns", dns)
                    .add_tag("endpoint", endpoint)
                    .add_tag("groupid", groupid)
                    .add_tag("host_group", host_group)
                    .add_tag("ip", row.get::<usize, String>(4))
                    .add_tag("port", row.get::<usize, String>(6))
                    .add_field("field", value);
        query
    }

    pub async fn sync_postgres(config: PostgreSQL, batch_size: usize, influx_client: &influxdb::Client, history_name: &str) -> bool{
        for i in config.zabbix_server.iter(){
            let connection_string = format!("host={host} user={user} password={password}",
                                                    user=&config.zabbix_username,
                                                    password=&config.zabbix_password,
                                                    host=&i);
            let (client, connection) = match tokio_postgres::connect(&connection_string, NoTls).await{
                                            Ok((pg_client, pg_connection)) => (pg_client, pg_connection),
                                            Err(e) => {
                                                eprintln!("Error connecting to PostgreSQL {}", e);
                                                continue
                                            },
                                        };
            tokio::spawn(async move {
                if let Err(e) = connection.await{
                    eprintln!("PostgreSQL connection error {}", e);
                }
            });
            let time_from = read_last_run(&format!("/var/lib/influxdb/zabbix-rust/{}", history_name));
            set_last_run(Utc::now().timestamp(), &format!("/var/lib/influxdb/zabbix-rust/{}", history_name));

            let query_string = format!(r#"SELECT hosts.name,
                                                items.key_,
                                                {history_name}.clock,
                                                {history_name}.value,
                                                interface.ip,
                                                interface.dns,
                                                interface.port,
                                                hstgrp.name,
                                                hosts_groups.groupid
                                                FROM hosts
                                                INNER JOIN items 
                                                ON items.hostid = hosts.hostid
                                                INNER JOIN interface
                                                ON interface.hostid = hosts.hostid
                                                INNER JOIN hosts_groups
                                                ON hosts_groups.hostid = hosts.hostid
                                                INNER JOIN hstgrp
                                                ON hstgrp.groupid = hosts_groups.groupid
                                                INNER JOIN {history_name}
                                                ON {history_name}.itemid = items.itemid
                                                WHERE {history_name}.clock >= {clock}
                                                ORDER BY {history_name}.clock ASC"#, 
                                                history_name=history_name, 
                                                clock=time_from);
            println!("Querying PostgreSQL for table {}", history_name);
            let rows = client.query(query_string.as_str(),&[]).await.unwrap();
            println!("Done Querying PostgreSQL for table {}", history_name);

            let rows_size = rows.len() as u64;
            let mut metrics: Vec<WriteQuery> = Vec::new();
            let mut progress: u64 = 0;
            
            for row in rows{
                let query = row_to_query(row, history_name);
                metrics.push(query);
                if metrics.len() >= batch_size{
                    progress += metrics.len() as u64;
                    let percentage = (progress * 100) / rows_size;
                    println!("Inserting batch into InfluxDB, {:.2}%", percentage);
                    let mut is_ok = false;
                    let mut tries = 0;
                    while !is_ok{
                        if tries >= 5{
                            set_last_run(time_from, "./last_run");
                            return false
                        }
                        tries += 1;
                        is_ok = database::influx::write_many_into_influx(metrics.clone(), influx_client).await;
                    }
                    metrics = Vec::new();
                }
            }
            database::influx::write_many_into_influx(metrics, influx_client).await;
            return true
        }
        false
    }
}