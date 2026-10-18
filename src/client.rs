//! `card-master place` and `card-master remove` as a person runs them: the request handed to the running
//! master on its loopback port, and what the master does said here as it does it
//! (docs/placement.md, 「A request」).

use axum::body::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper::header::{CONTENT_TYPE, HOST};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

use crate::placement::Verb;
use crate::serve::{DONE_LINE, FAILED_LINE};

/// The request sent and its lines printed as they come; whether it ended done, or why it could not ask.
pub async fn ask_master(port: u16, verb: Verb, service: &str, node: &str) -> Result<bool, String> {
  let stream = TcpStream::connect(("127.0.0.1", port))
    .await
    .map_err(|fault| format!("the card master does not answer on 127.0.0.1:{port}: {fault}"))?;
  let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
    .await
    .map_err(|fault| fault.to_string())?;
  tokio::spawn(connection);
  let body = serde_json::json!({ "service": service, "node": node }).to_string();
  let request = Request::post(format!("/{}", verb.word()))
    .header(HOST, format!("127.0.0.1:{port}"))
    .header(CONTENT_TYPE, "application/json")
    .body(Full::new(Bytes::from(body)))
    .map_err(|fault| fault.to_string())?;
  let mut answer = sender.send_request(request).await.map_err(|fault| fault.to_string())?;
  if !answer.status().is_success() {
    return Err(format!("the card master answered {}", answer.status()));
  }
  let mut pending = String::new();
  let mut last = String::new();
  while let Some(frame) = answer.body_mut().frame().await {
    let Ok(data) = frame.map_err(|fault| fault.to_string())?.into_data() else { continue };
    pending.push_str(&String::from_utf8_lossy(&data));
    while let Some((line, rest)) = pending.split_once('\n') {
      println!("{line}");
      last = line.to_owned();
      pending = rest.to_owned();
    }
  }
  if last.starts_with(DONE_LINE) {
    return Ok(true);
  }
  if last.starts_with(FAILED_LINE) {
    return Ok(false);
  }
  Err("the card master hung up before the request ended".into())
}
