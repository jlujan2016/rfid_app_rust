use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use tiberius::{Client, Config};
use tokio_util::compat::TokioAsyncWriteCompatExt;

use dotenvy::dotenv;
use std::env;
use std::sync::Arc;
use tokio::sync::Mutex;

use std::collections::HashMap;
use std::time::{Instant, Duration};

fn build_command(command: u8, data: &[u8]) -> Vec<u8> {
    let length = (8 + data.len()) as u16;

    let mut checksum: u8 =
        ((length >> 8) as u8) ^ (length as u8) ^ command;

    for b in data {
        checksum ^= b;
    }

    let mut frame = Vec::new();
    frame.push(0xA5);
    frame.push(0x5A);
    frame.push((length >> 8) as u8);
    frame.push(length as u8);
    frame.push(command);
    frame.extend_from_slice(data);
    frame.push(checksum);
    frame.push(0x0D);
    frame.push(0x0A);

    frame
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    // =========================
    // 🔌 SQL SERVER
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

    let client = Arc::new(Mutex::new(
        Client::connect(config, tcp.compat_write()).await?
    ));

    println!("✅ Conectado a SQL Server");

    // =========================
    // 📡 UR4 RFID
    // =========================
    let mut stream = TcpStream::connect(format!("{}:{}", rfid_host, rfid_port)).await?;
    println!("✅ Conectado al UR4");

    // modo lector
    stream.write_all(&build_command(0x60, &[0x01])).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // iniciar inventario
    stream.write_all(&build_command(0x82, &[0x00, 0x00])).await?;
    println!("📡 Inventario iniciado");

    let mut buffer: Vec<u8> = Vec::new();
    let mut temp = [0u8; 1024];

    let mut cache: HashMap<String, Instant> = HashMap::new();

    loop {
        let n = stream.read(&mut temp).await?;
        if n == 0 {
            continue;
        }

        buffer.extend_from_slice(&temp[..n]);

        let mut i = 0;

        while i + 4 < buffer.len() {
            if buffer[i] != 0xA5 || buffer[i + 1] != 0x5A {
                i += 1;
                continue;
            }

            let length = ((buffer[i + 2] as usize) << 8) | buffer[i + 3] as usize;

            if i + length > buffer.len() {
                break;
            }

            let frame = &buffer[i..i + length];

            // =========================
            // 🔥 UR4 INVENTORY FRAME
            // =========================
if frame.len() > 6 && frame[4] == 0x83 {

    // payload del lector
    let payload = &frame[5..frame.len().saturating_sub(2)];

    // buscar secuencia de EPC válida dentro del payload
    let mut found = String::new();

    for window in payload.windows(4) {

        // heurística UR4: EPC corto tipo 4 bytes
        let candidate = hex::encode(window);

        // filtro fuerte: solo valores tipo 00410002
        if candidate.starts_with("0041") || candidate.starts_with("0040") {

            found = candidate;
            break;
        }
    }

    if !found.is_empty() {

        let now = Instant::now();

        if let Some(last) = cache.get(&found) {
            if now.duration_since(*last) < Duration::from_secs(2) {
                i += length;
                continue;
            }
        }

        cache.insert(found.clone(), now);

        println!("📦 TAG LIMPIO: {}", found);

        guardar_epc(client.clone(), &found).await?;
    }
}

            i += length;
        }

        buffer.drain(0..i);
    }
}

// =========================
// 💾 SQL INSERT
// =========================
async fn guardar_epc(
    client: Arc<Mutex<Client<tokio_util::compat::Compat<TcpStream>>>>,
    epc: &str,
) -> anyhow::Result<()> {

    let query = "
        INSERT INTO LecturasRFID (EPC)
        SELECT @P1
        WHERE NOT EXISTS (
            SELECT 1 FROM LecturasRFID WHERE EPC = @P1
        )
    ";

    let mut client = client.lock().await;

    match client.execute(query, &[&epc]).await {
        Ok(r) => {
            if r.total() > 0 {
                println!("💾 INSERT OK: {}", epc);
            } else {
                println!("⚠️ DUPLICADO: {}", epc);
            }
        }
        Err(e) => {
            eprintln!("❌ SQL ERROR: {:?}", e);
        }
    }

    Ok(())
}