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

// =========================
// 🔧 CONSTRUIR COMANDO
// =========================
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

// =========================
// 🏷️ EXTRAER EPC
// =========================
fn extraer_epc(payload: &[u8]) -> Option<String> {
    // Necesitamos al menos 6 bytes para que haya algo entre
    // byte[2] y los ultimos 4 bytes de metadata
    if payload.len() < 6 {
        return None;
    }

    // El EPC siempre empieza en byte[2]
    // Los ultimos 4 bytes siempre son RSSI/metadata → los ignoramos
    let epc_start = 2;
    let epc_end   = payload.len() - 4;

    let epc = hex::encode(&payload[epc_start..epc_end]);

    Some(epc)
}

// =========================
// 🚀 MAIN
// =========================
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
    let mut stream = TcpStream::connect(
        format!("{}:{}", rfid_host, rfid_port)
    ).await?;
    println!("✅ Conectado al UR4");
    // modo lector
    stream.write_all(&build_command(0x60, &[0x01])).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    // iniciar inventario
    stream.write_all(&build_command(0x82, &[0x00, 0x00])).await?;
    println!("📡 Inventario iniciado");

    let mut buffer: Vec<u8> = Vec::new();
    let mut temp   = [0u8; 1024];
    let mut cache: HashMap<String, Instant> = HashMap::new();

    // --- LOOP PRINCIPAL ---
    loop {
        let n = stream.read(&mut temp).await?;
        if n == 0 {
            continue;
        }

        buffer.extend_from_slice(&temp[..n]);

        let mut i = 0;

        while i + 4 < buffer.len() {

            // Buscar cabecera A5 5A
            if buffer[i] != 0xA5 || buffer[i + 1] != 0x5A {
                i += 1;
                continue;
            }

            // Leer longitud de la trama
            let length = ((buffer[i + 2] as usize) << 8)
                        | buffer[i + 3] as usize;

            // Si no llegaron todos los bytes todavia, esperar
            if i + length > buffer.len() {
                break;
            }

            let frame = &buffer[i..i + length];
            // =========================
            // 🔥 UR4 INVENTORY FRAME
            // =========================
            // Es una trama de tag detectado (comando 0x83)
            if frame.len() > 6 && frame[4] == 0x83 {
                // payload del lector
                let payload = &frame[5..frame.len().saturating_sub(2)];

                if let Some(epc) = extraer_epc(payload) {
                    let now = Instant::now();

                    // Verificar cache: si el mismo tag fue leido
                    // hace menos de 2 segundos, ignorarlo
                    let es_reciente = cache
                        .get(&epc)
                        .map(|ultimo| now.duration_since(*ultimo)
                             < Duration::from_secs(2))
                        .unwrap_or(false);

                    if !es_reciente {
                        cache.insert(epc.clone(), now);
                        println!("📦 TAG DETECTADO: {}", epc);
                        guardar_epc(client.clone(), &epc).await?;
                    }
                }
            }

            i += length;
        }

        // Descartar bytes ya procesados
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
                println!("💾 INSERT OK : {}", epc);
            } else {
                println!("⚠️  YA EXISTE : {}", epc);
            }
        }
        Err(e) => {
            eprintln!("❌ SQL ERROR  : {:?}", e);
        }
    }

    Ok(())
}