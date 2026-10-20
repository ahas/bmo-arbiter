//! How the master reaches a node (docs/api.md, 「The node service」, 「Node shells」): the connection out of the
//! node service's target, the node service named by its settings, and an ssh command's quoting.

use std::path::Path;

use bmo_arbiter::reach::{Connection, NodeRegistry, NodeShell, Way, connection_of};
use bmo_node_client::nodes::{LoginShell, NodeKind, Target};

fn target(kind: NodeKind) -> Target {
  Target {
    node_name: "node-a".into(),
    kind,
    role: None,
    host: None,
    port: None,
    user: None,
    key_path: None,
    shell: None,
    ssh_options: Vec::new(),
    ssh_args: Vec::new(),
    hub_id: None,
    hub_address: None,
    hub_token_file: None,
    agent_name: None,
  }
}

#[test]
fn a_connection_is_read_from_the_node_services_target() {
  let hub = Target {
    role: Some("voice".into()),
    hub_id: Some(1),
    hub_address: Some("http://127.0.0.1:1".into()),
    hub_token_file: Some("~/t".into()),
    agent_name: Some("node-a".into()),
    ..target(NodeKind::Hub)
  };
  assert_eq!(
    connection_of(&hub),
    Ok(Connection {
      way: Way::Hub {
        agent: "node-a".into()
      },
      role: Some("voice".into())
    })
  );
  let ssh = Target {
    host: Some("h".into()),
    port: Some(22),
    user: Some("u".into()),
    shell: Some(LoginShell::Powershell),
    ssh_options: vec!["BatchMode=yes".into()],
    ssh_args: vec!["-o".into(), "BatchMode=yes".into(), "-p".into(), "22".into(), "u@h".into()],
    role: Some(String::new()),
    ..target(NodeKind::Ssh)
  };
  let read = connection_of(&ssh).unwrap();
  assert_eq!(
    read.way,
    Way::Ssh {
      host: "h".into(),
      port: 22,
      user: "u".into(),
      key_path: None,
      ssh_args: ssh.ssh_args.clone(),
      ssh_options: ssh.ssh_options.clone(),
      powershell: true
    }
  );
  assert_eq!(read.role, None);
  assert!(connection_of(&target(NodeKind::Hub)).unwrap_err().contains("agentName"));
  assert!(connection_of(&target(NodeKind::Ssh)).unwrap_err().contains("host"));
}

#[test]
fn the_node_service_is_named_by_its_address() {
  assert!(NodeRegistry::at("http://127.0.0.1:8790", "token").is_ok());
  assert!(NodeRegistry::at("no address", "token").unwrap_err().contains("CARD_NODE_URL"));
}

#[test]
fn an_ssh_command_runs_in_bash_with_its_quotes_kept_over_the_targets_login() {
  let shell = NodeShell {
    connection: Connection {
      way: Way::Ssh {
        host: "h".into(),
        port: 2222,
        user: "u".into(),
        key_path: Some("/k".into()),
        ssh_args: ["-o", "BatchMode=yes", "-i", "/k", "-p", "2222", "u@h"].map(String::from).to_vec(),
        ssh_options: vec!["BatchMode=yes".into()],
        powershell: false,
      },
      role: None,
    },
  };
  let argv = shell.exec_argv("echo 'a b'").unwrap();
  assert_eq!(argv[..8], ["ssh", "-o", "BatchMode=yes", "-i", "/k", "-p", "2222", "u@h"]);
  assert_eq!(argv.last().unwrap(), "bash -c 'echo '\\''a b'\\'''");
  let upload = shell.upload_argv(Path::new("/l"), "/r").unwrap();
  assert_eq!(upload, ["scp", "-q", "-o", "BatchMode=yes", "-i", "/k", "-P", "2222", "/l", "u@h:/r"]);
}
