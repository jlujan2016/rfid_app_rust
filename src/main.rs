use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Conectando a SQL Server...");

    let mut config = Config::new();
    config.host("0_Ciberelectrik.mssql.somee.com");
    config.port(1433);
    config.authentication(tiberius::AuthMethod::sql_server("jlujan_SQLLogin_1", "yyeftklvtf"));
    config.trust_cert();

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp.compat_write()).await?;

    println!("Conectado a SQL Server ✅");

    let epc = "E2000017221101441890TEST";

    client.execute(
        "INSERT INTO Lecturas (EPC) VALUES (@P1)",
        &[&epc],
    ).await?;

    println!("Dato insertado ✅");

    Ok(())
}