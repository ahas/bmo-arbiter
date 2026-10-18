//! How the master reaches a node (docs/api.md, 「The node registry」, 「Node shells」): the connection out
//! of the registry command's last line, the node names out of its listing, and an ssh command's quoting.

use std::path::Path;

use bmo_arbiter::reach::{Connection, NodeRegistry, NodeShell, Way, parse_connection, parse_node_list};

#[test]
fn a_connection_is_read_from_the_registry_commands_last_line() {
  let console = parse_connection(
    "noise\n{\"kind\":\"hub\",\"hubUrl\":\"http://127.0.0.1:1\",\"tokenFile\":\"~/t\",\"agent\":\"node-a\",\"role\":\"voice\"}\n",
  );
  assert_eq!(
    console,
    Some(Connection {
      way: Way::Hub {
        agent: "node-a".into()
      },
      role: Some("voice".into())
    })
  );
  let ssh =
    parse_connection(r#"{"kind":"ssh","host":"h","port":22,"user":"u","shell":"powershell"}"#).unwrap();
  assert_eq!(
    ssh.way,
    Way::Ssh {
      host: "h".into(),
      port: 22,
      user: "u".into(),
      key_path: None,
      powershell: true
    }
  );
  assert_eq!(ssh.role, None);
  assert_eq!(parse_connection(r#"{"kind":"hub","agent":"node-a"}"#), None);
  assert_eq!(parse_connection("not json"), None);
}

#[test]
fn the_node_names_are_read_from_the_registrys_listing_and_its_command_from_its_setting() {
  let listing = "desktop-pc [ssh] desktop — user@example-host:2222\n    connected · checked\nnode-a [hub] node-a — node-a @ http://x\n\nhubs:\n";
  assert_eq!(parse_node_list(listing), ["desktop-pc", "node-a"]);
  let registry = NodeRegistry::from_words("'/opt/my tools/bmo' nodes").unwrap();
  assert_eq!(
    registry.argv(&["--target", "node-a"]),
    ["/opt/my tools/bmo", "nodes", "--target", "node-a"]
  );
  assert!(NodeRegistry::from_words("  ").unwrap_err().contains("CARD_NODES_COMMAND"));
}

#[test]
fn an_ssh_command_runs_in_bash_with_its_quotes_kept() {
  let shell = NodeShell {
    connection: Connection {
      way: Way::Ssh {
        host: "h".into(),
        port: 2222,
        user: "u".into(),
        key_path: Some("/k".into()),
        powershell: false,
      },
      role: None,
    },
  };
  let argv = shell.exec_argv("echo 'a b'").unwrap();
  assert_eq!(argv.last().unwrap(), "bash -c 'echo '\\''a b'\\'''");
  assert!(argv.windows(2).any(|pair| pair == ["-p", "2222"]));
  assert!(argv.windows(2).any(|pair| pair == ["-i", "/k"]));
  let upload = shell.upload_argv(Path::new("/l"), "/r").unwrap();
  assert!(upload.windows(2).any(|pair| pair == ["-P", "2222"]));
}
