use tokio::net::TcpStream;
use tokio::io::AsyncReadExt;

use tiberius::{Client, Config};
use tokio_util::compat::TokioAsyncWriteCompatExt;
use dotenvy::dotenv;
use std::env;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
     dotenv().ok(); // 👈 cargar variables .env
    // =========================
    // 🔌 CONEXIÓN SQL SERVER
    // =========================
    let mut config = Config::new();
    let db_host = env::var("DB_HOST")?;
    let db_port = env::var("DB_PORT")?.parse::<u16>()?;
    let db_name = env::var("DB_NAME")?;
    let db_user = env::var("DB_USER")?;
    let db_pass = env::var("DB_PASS")?;

    let rfid_host = env::var("RFID_HOST")?;
    let rfid_port = env::var("RFID_PORT")?.parse::<u16>()?;

    config.host(&db_host);
    config.port(db_port);
    config.authentication(tiberius::AuthMethod::sql_server(&db_user, &db_pass));
    config.database(&db_name);
    config.trust_cert();

    let tcp = TcpStream::connect(format!("{}:{}", db_host, db_port)).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp.compat_write()).await?;

    println!("✅ Conectado a SQL Server");

    // =========================
    // 📡 CONEXIÓN UR4
    // =========================
    let mut stream = TcpStream::connect(format!("{}:{}", rfid_host, rfid_port)).await?;
    println!("✅ Conectado al UR4");

    let mut buffer = [0u8; 1024];

    loop {
        let n = stream.read(&mut buffer).await?;
        let data = &buffer[..n];

        println!("RAW: {:?}", data);

        if let Some(epc) = extract_epc(data) {
            println!("📦 TAG: {}", epc);

            guardar_epc(&mut client, &epc).await?;
        }
    }
}

// =========================
// 🧠 EXTRAER EPC
// =========================
fn extract_epc(data: &[u8]) -> Option<String> {
    if data.len() > 5 {
        let epc_bytes = &data[5..];
        Some(hex::encode(epc_bytes))
    } else {
        None
    }
}

// =========================
// 💾 INSERTAR EN SQL
// =========================
async fn guardar_epc(
    client: &mut Client<tokio_util::compat::Compat<tokio::net::TcpStream>>,
    epc: &str,
) -> anyhow::Result<()> {

    let query = "
        IF NOT EXISTS (SELECT 1 FROM LecturasRFID WHERE EPC = @P1)
        INSERT INTO LecturasRFID (EPC) VALUES (@P1)
    ";

    client.execute(query, &[&epc]).await?;

    println!("💾 Guardado en SQL");

    Ok(())
}