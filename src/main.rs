use tokio::net::TcpStream;
use tokio::io::AsyncReadExt;

use tiberius::{Client, Config};
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // =========================
    // 🔌 CONEXIÓN SQL SERVER
    // =========================
    let mut config = Config::new();
    config.host("0_Ciberelectrik.mssql.somee.com");
    config.port(1433);
    config.authentication(tiberius::AuthMethod::sql_server("jlujan_SQLLogin_1", "yyeftklvtf"));
    config.database("0_Ciberelectrik");
    config.trust_cert();

    let tcp = TcpStream::connect("0_Ciberelectrik.mssql.somee.com:1433").await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp.compat_write()).await?;

    println!("✅ Conectado a SQL Server");

    // =========================
    // 📡 CONEXIÓN UR4
    // =========================
    let mut stream = TcpStream::connect("127.0.0.1:5084").await?;
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