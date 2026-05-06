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

    let client = Arc::new(Mutex::new(
    Client::connect(config, tcp.compat_write()).await?
    ));

    println!("✅ Conectado a SQL Server");

    // =========================
    // 📡 CONEXIÓN UR4
    // =========================
    let mut stream = TcpStream::connect(format!("{}:{}", rfid_host, rfid_port)).await?;
    println!("✅ Conectado al UR4");
// 🔥 1. poner modo lector (IMPORTANTE)
stream.write_all(&build_command(0x60, &[0x01])).await?;
println!("⚙️ Modo inventario configurado");

// pequeño delay recomendado
tokio::time::sleep(std::time::Duration::from_millis(200)).await;

// 🔥 2. iniciar inventario
let start_inventory = build_command(0x82, &[0x00, 0x00]);
stream.write_all(&start_inventory).await?;
println!("📡 Inventario iniciado");

    let mut buffer: Vec<u8> = Vec::new();
    let mut temp = [0u8; 1024];
    use std::collections::HashMap;
use std::time::Instant;

let mut cache: HashMap<String, Instant> = HashMap::new();

    // 📡 loop de lectura
loop {
    let n = stream.read(&mut temp).await?;
    buffer.extend_from_slice(&temp[..n]);

    println!("RAW: {:?}", &temp[..n]);

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

        if frame.len() > 5 && frame[4] == 0x83 {
            let epc_data = &frame[5..frame.len().saturating_sub(3)];

            if !epc_data.is_empty() {
                let epc = hex::encode(epc_data);

                let now = Instant::now();

                if let Some(last) = cache.get(&epc) {
                    if now.duration_since(*last) < Duration::from_secs(2) {
                        i += length;
                        continue;
                    }
                }

                cache.insert(epc.clone(), now);

                println!("📦 TAG: {}", epc);

                guardar_epc(client.clone(), &epc).await?;
            }
        }

        i += length;
    }

    buffer.drain(0..i);
}
}
// =========================
// 🧠 EXTRAER EPC
// =========================
fn extract_all_epcs(data: &[u8]) -> Vec<String> {
    let mut epcs = Vec::new();
    let mut i = 0;

    while i + 4 < data.len() {
        // Buscar inicio de frame A5 5A
        if data[i] != 0xA5 || data[i + 1] != 0x5A {
            i += 1;
            continue;
        }

        // Leer longitud del frame
        let length = ((data[i + 2] as usize) << 8) | data[i + 3] as usize;

        // Validar que el frame completo está en el buffer
        if i + length > data.len() {
            break; // frame incompleto, esperar más datos
        }

        let frame = &data[i..i + length];

        // Buscar EPC (0xE2 + 12 bytes) dentro del frame
        if let Some(rel_pos) = frame.windows(1).position(|w| w[0] == 0xE2) {
            if rel_pos + 12 <= frame.len() {
                let epc = hex::encode(&frame[rel_pos..rel_pos + 12]);

                // Evitar duplicados dentro del mismo chunk
                if !epcs.contains(&epc) {
                    epcs.push(epc);
                }
            }
        }

        i += length; // saltar al siguiente frame
    }

    epcs
}

// =========================
// 💾 INSERTAR EN SQL
// =========================
async fn guardar_epc(
    client: Arc<Mutex<Client<tokio_util::compat::Compat<tokio::net::TcpStream>>>>,
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
            let rows = r.total();
            if rows == 0 {
                println!("⚠️ NO INSERT (posible duplicado o filtro): {}", epc);
            } else {
                println!("💾 INSERT OK: {}", epc);
            }
        }
        Err(e) => {
            eprintln!("❌ SQL ERROR: {:?}", e);
        }
    }

    Ok(())
}