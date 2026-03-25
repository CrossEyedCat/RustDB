//! Minimal QUIC client: send one SQL query and print the response frame (dev / manual testing).
//!
//! Usage:
//! ```text
//! rustdb_quic_client --addr 127.0.0.1:5432 --cert server.der "SELECT 1"
//! ```
//!
//! Save the server leaf certificate in DER form (`pinned_certificate` from a test or future export) as `server.der`.

use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use rustls::pki_types::CertificateDer;

use rustdb::network::client::{build_quinn_client_config, connect, make_client_endpoint, query_once};
use rustdb::network::framing::ServerMessage;

#[derive(Parser, Debug)]
#[command(name = "rustdb_quic_client")]
struct Args {
    /// Server address (host:port).
    #[arg(long, default_value = "127.0.0.1:5432")]
    addr: String,
    /// Path to the server leaf certificate (DER), for trusting the dev self-signed cert.
    #[arg(long)]
    cert: PathBuf,
    /// TLS server name (must match certificate SAN; use `127.0.0.1` when the server cert is for that IP).
    #[arg(long, default_value = "127.0.0.1")]
    server_name: String,
    /// SQL query text.
    sql: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let addr: SocketAddr = args.addr.parse()?;
    let der = fs::read(&args.cert)?;
    let cert = CertificateDer::from(der);
    let client_cfg = build_quinn_client_config(std::slice::from_ref(&cert))?;
    let endpoint = make_client_endpoint(client_cfg)?;
    let conn = connect(&endpoint, addr, &args.server_name).await?;
    let msg = query_once(&conn, &args.sql).await?;
    match msg {
        ServerMessage::ResultSet(p) => {
            println!("ResultSet: columns={:?} rows={:?}", p.columns, p.rows);
        }
        ServerMessage::ExecutionOk(p) => {
            println!("ExecutionOk: rows_affected={}", p.rows_affected);
        }
        ServerMessage::Error(p) => {
            println!("Error: code={} message={}", p.code, p.message);
        }
        ServerMessage::ServerReady(p) => {
            println!("ServerReady: {}", p.server_version);
        }
    }
    Ok(())
}
